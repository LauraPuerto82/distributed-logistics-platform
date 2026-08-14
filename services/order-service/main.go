package main

import (		
	"net/http"

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

func main() {
	router := gin.Default()

	shipments := make(map[string]Shipment)

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

		shipments[shipment.ID] = shipment
	
		c.JSON(http.StatusCreated, shipment)
	})

	router.GET("/shipments/:id", func(c *gin.Context) {
		id := c.Param("id")
	
		shipment, exists := shipments[id]
		if !exists {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "shipment not found",
			})
			return
		}
	
		c.JSON(http.StatusOK, shipment)
	})

	router.Run(":8080")
}



