package metrics
import (
	"net/http"
	"strconv"
	"time"
	"github.com/go-chi/chi/v5/middleware"
)

func HTTPMetricMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w,r.ProtoMajor)

		next.ServeHTTP(ww,r)
		duration := time.Since(start).Seconds()
		statusStr:= strconv.Itoa(ww.Status())

		HTTPRequestDuration.WithLabelValues(r.Method,r.URL.Path,statusStr).Observe(duration)
	}) 
}