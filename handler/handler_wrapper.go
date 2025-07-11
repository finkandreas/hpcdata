package handler

import (
	"net/http"
)

type CanGet interface {
	Get(http.ResponseWriter, *http.Request)
}
type CanPost interface {
	Post(http.ResponseWriter, *http.Request)
}
type CanPut interface {
	Put(http.ResponseWriter, *http.Request)
}
type CanDelete interface {
	Delete(http.ResponseWriter, *http.Request)
}
type AnyMethod interface {
	Handle(http.ResponseWriter, *http.Request)
}
type HandlerWrapper struct {
	RealHandler interface{}
}

func wrap(handler interface{}) func(w http.ResponseWriter, r *http.Request) {
	wrapper := &HandlerWrapper{handler}
	return wrapper.handle
}
func (h *HandlerWrapper) handle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if GetHandler, ok := h.RealHandler.(CanGet); ok {
			GetHandler.Get(w, r)
		} else {
			http.Error(w, "Method not supoorted", http.StatusMethodNotAllowed)
		}
	case "POST":
		if PostHandler, ok := h.RealHandler.(CanPost); ok {
			PostHandler.Post(w, r)
		} else {
			http.Error(w, "Method not supoorted", http.StatusMethodNotAllowed)
		}
	case "PUT":
		if PutHandler, ok := h.RealHandler.(CanPut); ok {
			PutHandler.Put(w, r)
		} else {
			http.Error(w, "Method not supoorted", http.StatusMethodNotAllowed)
		}
	case "DELETE":
		if DeleteHandler, ok := h.RealHandler.(CanDelete); ok {
			DeleteHandler.Delete(w, r)
		} else {
			http.Error(w, "Method not supoorted", http.StatusMethodNotAllowed)
		}
	default:
		if DefaultHandler, ok := h.RealHandler.(AnyMethod); ok {
			DefaultHandler.Handle(w, r)
		} else {
			http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		}
	}
}
