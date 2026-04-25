// Package grpc hosts the data-service gRPC handler layer. Each file
// wraps one logical service surface (data sources, queries, dashboards,
// vocabulary) so the BFF can speak gRPC instead of HTTP.
//
// Most CRUD logic is implemented directly here using GORM, mirroring
// the existing HTTP handlers. The deep paths (SyncSchema, ExecuteDataQuery,
// NaturalLanguageQuery, RotateDashboardEmbedToken, GetDashboardByEmbedToken)
// delegate to the HTTP handler via an in-process bridge until a proper
// service-layer extraction lands. See deep_bridge.go.
package grpc

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/gorm"

	datav1 "github.com/sentiae/data-service/gen/data/v1"
	"github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/platform-kit/timetravel"
)

// ServerConfig captures the bind address.
type ServerConfig struct {
	Host string
	Port string
}

// Deps bundles every dependency the gRPC handlers need.
type Deps struct {
	DB       *gorm.DB
	Pub      kafka.Publisher
	Recorder timetravel.Recorder
	// HTTPHandler is the existing chi router. The deep-path bridge
	// dispatches synthetic requests through it for SyncSchema /
	// ExecuteDataQuery / NaturalLanguageQuery / Embed token paths
	// until those flows are extracted into a shared service layer.
	HTTPHandler http.Handler
}

// Server is the composite gRPC server.
type Server struct {
	cfg        ServerConfig
	grpcServer *grpc.Server
	healthSrv  *health.Server
	listener   net.Listener
}

// NewServer wires the dependencies into gRPC handlers.
func NewServer(cfg ServerConfig, deps Deps) *Server {
	if deps.Pub == nil {
		deps.Pub = kafka.NewNoopPublisher()
	}
	if deps.Recorder == nil {
		deps.Recorder = timetravel.NoopRecorder{}
	}

	grpcSrv := grpc.NewServer()

	dataSourceSvc := NewDataSourceServiceServer(deps)
	dataQuerySvc := NewDataQueryServiceServer(deps)
	dashboardSvc := NewDashboardServiceServer(deps)
	vocabularySvc := NewVocabularyServiceServer(deps)

	datav1.RegisterDataSourceServiceServer(grpcSrv, dataSourceSvc)
	datav1.RegisterDataQueryServiceServer(grpcSrv, dataQuerySvc)
	datav1.RegisterDashboardServiceServer(grpcSrv, dashboardSvc)
	datav1.RegisterVocabularyServiceServer(grpcSrv, vocabularySvc)

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	for _, name := range serviceNames() {
		healthSrv.SetServingStatus(name, grpc_health_v1.HealthCheckResponse_SERVING)
	}
	reflection.Register(grpcSrv)

	return &Server{
		cfg:        cfg,
		grpcServer: grpcSrv,
		healthSrv:  healthSrv,
	}
}

func serviceNames() []string {
	return []string{
		"data.v1.DataSourceService",
		"data.v1.DataQueryService",
		"data.v1.DashboardService",
		"data.v1.VocabularyService",
	}
}

// Addr returns the bound address.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Start binds the listener and serves until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	host := s.cfg.Host
	port := s.cfg.Port
	if port == "" {
		port = "50060"
	}
	addr := fmt.Sprintf("%s:%s", host, port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen on %s failed: %w", addr, err)
	}
	s.listener = lis

	go func() {
		<-ctx.Done()
		s.markNotServing()
		s.grpcServer.GracefulStop()
	}()

	if err := s.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("grpc serve failed: %w", err)
	}
	return nil
}

// GracefulStop drains in-flight RPCs.
func (s *Server) GracefulStop() {
	if s.grpcServer == nil {
		return
	}
	s.markNotServing()
	s.grpcServer.GracefulStop()
}

func (s *Server) markNotServing() {
	if s.healthSrv == nil {
		return
	}
	s.healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	for _, name := range serviceNames() {
		s.healthSrv.SetServingStatus(name, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}
}
