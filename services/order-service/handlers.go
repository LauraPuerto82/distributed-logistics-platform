package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Shipment struct {
	ID          string  `json:"id"`
	Origin      string  `json:"origin" binding:"required"`
	Destination string  `json:"destination" binding:"required"`
	Weight      float64 `json:"weight" binding:"required,gt=0"`
	Priority    string  `json:"priority" binding:"required,oneof=LOW MEDIUM HIGH"`
	Status      string  `json:"status"`
}

func setupRouter(
	store ShipmentStore,
	publisher EventPublisher,
) *gin.Engine {
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	router.POST("/shipments", func(c *gin.Context) {
		var shipment Shipment

		if err := c.ShouldBindJSON(&shipment); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}

		if shipment.Origin == shipment.Destination {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "origin and destination must be different",
			})
			return
		}

		shipment.ID = "shp_" + uuid.NewString()
		shipment.Status = "CREATED"

		// Persist before publishing the event.
		// This is intentionally non-atomic for the current MVP; see ADR-008.
		if err := store.Save(shipment); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to save shipment",
			})
			return
		}

		event := ShipmentCreatedEvent{
			EventID:    "evt_" + uuid.NewString(),
			EventType:  "ShipmentCreated",
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			ShipmentID: shipment.ID,
			Payload: ShipmentCreatedPayload{
				Origin:      shipment.Origin,
				Destination: shipment.Destination,
				Weight:      shipment.Weight,
				Priority:    shipment.Priority,
			},
		}

		if err := publisher.PublishShipmentCreated(event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to publish shipment created event",
			})
			return
		}

		c.JSON(http.StatusCreated, shipment)
	})

	router.GET("/shipments/:id", func(c *gin.Context) {
		id := c.Param("id")

		shipment, exists, err := store.GetByID(id)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to get shipment",
			})
			return
		}

		if !exists {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "shipment not found",
			})
			return
		}

		c.JSON(http.StatusOK, shipment)
	})

	return router
}
