package httpapi

import (
	"encoding/json"
	"net/http"
	"regexp"
	"reliable-webhook-dispatcher/internal/store"
)

var (
	amountRe = regexp.MustCompile(`^[0-9]+(\.[0-9]{1,2})?$`)
	uuidRe   = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

type CreateOrderRequest struct {
	CustomerID string `json:"customer_id"`
	Amount     string `json:"amount"`
}

type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}

type OrderHandler struct {
	Orders *store.OrderStore
}

func (h *OrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if !uuidRe.MatchString(req.CustomerID) {
		writeError(w, http.StatusBadRequest, "invalid customer_id")
		return
	}
	if !amountRe.MatchString(req.Amount) {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	res, err := h.Orders.CreateOrderWithOutbox(r.Context(), store.CreateOrderInput{
		CustomerID: req.CustomerID,
		Amount:     req.Amount,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create order failed")
		return
	}

	writeJSON(w, http.StatusCreated, CreateOrderResponse{
		OrderID: res.OrderID,
		EventID: res.EventID,
		Status:  res.Status,
	})
}

type OutboxHandler struct {
	Outbox *store.OutboxStore
}

func (h *OutboxHandler) List(w http.ResponseWriter, r *http.Request) {
	events, err := h.Outbox.List(r.Context(), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list outbox failed")
		return
	}

	writeJSON(w, http.StatusOK, events)
}
