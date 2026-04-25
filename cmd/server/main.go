package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	// Register pgx as database/sql driver for direct SQL queries
	_ "github.com/jackc/pgx/v5/stdlib"

	// Register MySQL driver so federated adapters can connect to MySQL-backed sources.
	_ "github.com/go-sql-driver/mysql"

	grpchandler "github.com/sentiae/data-service/internal/handler/grpc"
	handler "github.com/sentiae/data-service/internal/handler/http"
	"github.com/sentiae/data-service/internal/infrastructure/messaging"
	pgmigrations "github.com/sentiae/data-service/internal/repository/postgres"
	pkkafka "github.com/sentiae/platform-kit/kafka"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// maybeRegisterKafkaSchemas runs the G17 schema-registry bootstrap.
// Gated by APP_KAFKA_REGISTER_SCHEMAS_ON_BOOT=true.
func maybeRegisterKafkaSchemas() {
	if os.Getenv("APP_KAFKA_REGISTER_SCHEMAS_ON_BOOT") != "true" {
		return
	}
	url := os.Getenv("APP_KAFKA_SCHEMA_REGISTRY_URL")
	if url == "" {
		return
	}
	prefix := os.Getenv("APP_KAFKA_TOPIC_PREFIX")
	if prefix == "" {
		prefix = "sentiae"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := pkkafka.NewSchemaRegistry(url)
	result := pkkafka.RegisterAllSchemas(ctx, registry, prefix)
	if len(result.Errors) > 0 {
		log.Printf("schema-registry bootstrap: registered=%d skipped=%d errors=%d (first: %s)",
			result.Registered, result.Skipped, len(result.Errors), result.Errors[0])
		return
	}
	log.Printf("schema-registry bootstrap: registered %d schemas", result.Registered)
}

func main() {
	go maybeRegisterKafkaSchemas()
	dbURL := getEnv("DATABASE_URL", "postgres://sentiae:sentiae@localhost:5432/sentiae_data?sslmode=disable")
	port := getEnv("PORT", "8088")
	autoMigrate := getEnv("APP_DATABASE_AUTO_MIGRATE", "true")

	db, err := gorm.Open(pgdriver.Open(dbURL), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if autoMigrate == "true" {
		if err := pgmigrations.AutoMigrate(db); err != nil {
			log.Fatalf("Auto-migration failed: %v", err)
		}
		log.Println("Database migration completed")
	}

	// Initialize Kafka publisher (falls back to noop when disabled).
	kafkaEnabled := strings.EqualFold(getEnv("APP_KAFKA_ENABLED", "false"), "true")
	brokers := strings.Split(getEnv("APP_KAFKA_BROKERS", "localhost:9092"), ",")
	pub := messaging.InitFromEnv(messaging.Config{
		Enabled:     kafkaEnabled,
		Brokers:     brokers,
		TopicPrefix: getEnv("APP_KAFKA_TOPIC_PREFIX", "sentiae"),
	})
	defer pub.Close()

	server := handler.NewServer(db, pub)
	defer server.Close()

	// Start the gRPC server alongside HTTP. Default-on so the BFF can
	// dial in dev; gate behind APP_GRPC_ENABLED=false to disable.
	ctx, cancelGRPC := context.WithCancel(context.Background())
	defer cancelGRPC()
	grpcSrv := startGRPCServer(ctx, db, pub, server)

	addr := fmt.Sprintf(":%s", port)
	log.Printf("Data service starting on %s", addr)
	httpServer := &http.Server{Addr: addr, Handler: server}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Block on whichever server exits first. The HTTP server will only
	// return ErrServerClosed via Shutdown, so the goroutine stays alive
	// for the lifetime of the process. We keep the gRPC Start in the
	// foreground when enabled so a binding failure surfaces immediately.
	if grpcSrv != nil {
		// Start blocks; on cancellation Start returns nil.
		if err := grpcSrv.Start(ctx); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	} else {
		// Wait forever — HTTP goroutine handles signals upstream.
		select {}
	}
}

// startGRPCServer wires the gRPC handler with the HTTP router, recorder,
// and publisher so the deep-path bridge can reuse them.
func startGRPCServer(ctx context.Context, db *gorm.DB, pub pkkafka.Publisher, httpSrv *handler.Server) *grpchandler.Server {
	if strings.EqualFold(getEnv("APP_GRPC_ENABLED", "true"), "false") {
		return nil
	}
	grpcHost := getEnv("APP_GRPC_HOST", "")
	grpcPort := getEnv("APP_GRPC_PORT", "50060")
	srv := grpchandler.NewServer(grpchandler.ServerConfig{
		Host: grpcHost,
		Port: grpcPort,
	}, grpchandler.Deps{
		DB:          db,
		Pub:         pub,
		Recorder:    httpSrv.Recorder(),
		HTTPHandler: httpSrv,
	})
	log.Printf("gRPC server bound to %s:%s", grpcHost, grpcPort)
	return srv
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
