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

	addr := fmt.Sprintf(":%s", port)
	log.Printf("Data service starting on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
