package handler

import (
	"net/http"
)

type CatchAllHandler struct{}

func (h CatchAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "404 page not found", http.StatusNotFound)
}
