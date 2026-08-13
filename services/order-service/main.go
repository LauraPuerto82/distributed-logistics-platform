package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Shipment struct {
	ID          string  `json:"id"`
	Origin      string  `json:"origin"`
	Destination string  `json:"destination"`
	Weight      float64 `json:"weight"`
	Priority    string  `json:"priority"`
	Status      string  `json:"status"`
}

func main() {
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	router.Run(":8080")
}