package main

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"os"
	"strconv"
	"time"
)

func initTracing(ctx context.Context) (func(), error) {
	enabled := os.Getenv("ADX_TRACE_ENABLED")
	if enabled != "" && enabled != "true" && enabled != "false" {
		return nil, errors.New("ADX_TRACE_ENABLED must be true or false")
	}
	ratio := 1.0
	if text := os.Getenv("ADX_TRACE_SAMPLE_RATIO"); text != "" {
		v, err := strconv.ParseFloat(text, 64)
		if err != nil || !(v >= 0 && v <= 1) {
			return nil, errors.New("ADX_TRACE_SAMPLE_RATIO must be in [0,1]")
		}
		ratio = v
	}
	sampler := sdktrace.NeverSample()
	if enabled == "true" {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
	options := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sampler), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "adx-sandbox-api")))}
	if enabled == "true" {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithTimeout(2*time.Second))
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}, nil
}

func traceHTTP() gin.HandlerFunc {
	return func(c *gin.Context) {
		parent := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		route := c.FullPath()
		if route == "" {
			route = "<unmatched>"
		}
		ctx, span := otel.Tracer("adx").Start(parent, "sandbox-api.http", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("http.request.method", c.Request.Method), attribute.String("http.route", route)))
		defer span.End()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		span.SetAttributes(attribute.Int("http.response.status_code", c.Writer.Status()))
		if c.Writer.Status() >= 500 {
			span.SetStatus(codes.Error, "request failed")
		}
	}
}

type metadataCarrier metadata.MD

func (m metadataCarrier) Get(key string) string {
	v := metadata.MD(m).Get(key)
	if len(v) > 0 {
		return v[0]
	}
	return ""
}
func (m metadataCarrier) Set(key, value string) { metadata.MD(m).Set(key, value) }
func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
func traceUnary(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
	ctx, span := otel.Tracer("adx").Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	otel.GetTextMapPropagator().Inject(ctx, metadataCarrier(md))
	err := invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, options...)
	if err != nil {
		span.SetStatus(codes.Error, "RPC failed")
	}
	return err
}
