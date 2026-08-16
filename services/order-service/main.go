package main

import (		
	"net/http"
	"context"
    "log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Shipment struct {
	ID          string  `json:"id"`
	Origin      string  `json:"origin" binding:"required"`
	Destination string  `json:"destination" binding:"required"`
	Weight      float64 `json:"weight" binding:"required,gt=0"`
	Priority    string  `json:"priority" binding:"required,oneof=LOW MEDIUM HIGH"`
	Status      string  `json:"status"`
}

type ShipmentStore interface {
	Save(shipment Shipment) error
	GetByID(id string) (Shipment, bool, error)
}

type InMemoryShipmentStore struct {
	shipments map[string]Shipment
}

type PostgresShipmentStore struct {
	conn *pgx.Conn
}

func NewInMemoryShipmentStore() *InMemoryShipmentStore {
	return &InMemoryShipmentStore{
		shipments: make(map[string]Shipment),
	}
}

func NewPostgresShipmentStore(conn *pgx.Conn) *PostgresShipmentStore {
	return &PostgresShipmentStore{
		conn: conn,
	}
}

func (s *InMemoryShipmentStore) Save(shipment Shipment) error {
	s.shipments[shipment.ID] = shipment
	return nil
}

func (s *PostgresShipmentStore) Save(shipment Shipment) error {
	query := `
		INSERT INTO shipments (
			id,
			origin,
			destination,
			weight,
			priority,
			status
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := s.conn.Exec(
		context.Background(),
		query,
		shipment.ID,
		shipment.Origin,
		shipment.Destination,
		shipment.Weight,
		shipment.Priority,
		shipment.Status,
	)

	return err
}

func (s *InMemoryShipmentStore) GetByID(id string) (Shipment, bool, error) {
	shipment, exists := s.shipments[id]
	return shipment, exists, nil
}

func (s *PostgresShipmentStore) GetByID(id string) (Shipment, bool, error) {
	query := `
		SELECT id, origin, destination, weight, priority, status
		FROM shipments
		WHERE id = $1
	`

	var shipment Shipment

	err := s.conn.QueryRow(
		context.Background(),
		query,
		id,
	).Scan(
		&shipment.ID,
		&shipment.Origin,
		&shipment.Destination,
		&shipment.Weight,
		&shipment.Priority,
		&shipment.Status,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return Shipment{}, false, nil
		}

		return Shipment{}, false, err
	}

	return shipment, true, nil
}

func setupRouter(store ShipmentStore) *gin.Engine {
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

		if err := store.Save(shipment); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to save shipment",
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

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")

    if databaseURL == "" {
	    log.Fatal("DATABASE_URL is not set")
    }

	conn, err := pgx.Connect(ctx, databaseURL)	

	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL:", err)
	}

	defer conn.Close(ctx)

	if err := conn.Ping(ctx); err != nil {
		log.Fatal("PostgreSQL ping failed:", err)
	}

	log.Println("Connected to PostgreSQL")

	//store := NewInMemoryShipmentStore()
	store := NewPostgresShipmentStore(conn)

	router := setupRouter(store)
	router.Run(":8080")
}



