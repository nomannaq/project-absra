package auth

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/nomannaq/absra/internal/config"
)

// ClientIDKey is the context key used to store the client ID
type contextKey string

const ClientIDKey contextKey = "clientID"

// Service handles authentication operations
type Service struct {
	config config.AuthConfig
}

// Credentials represent authentication credentials
type Credentials struct {
	ClientID string `json:"client_id" binding:"required"`
	Secret   string `json:"secret" binding:"required"`
}

// Claims represents JWT token claims
type Claims struct {
	ClientID string `json:"client_id"`
	jwt.RegisteredClaims
}

// NewService creates a new authentication service
func NewService(config config.AuthConfig) *Service {
	return &Service{
		config: config,
	}
}

// IssueToken generates a new JWT token
func (s *Service) IssueToken(c *gin.Context) {
	var creds Credentials
	if err := c.ShouldBindJSON(&creds); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
		return
	}

	// In production, validate credentials against a database
	// For this example, we accept any client ID with a matching secret
	// In real applications, replace this with actual authentication

	// Create token expiration time
	expirationTime := time.Now().Add(s.config.TokenExpiration)

	// Create claims
	claims := &Claims{
		ClientID: creds.ClientID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "absra-event-bus",
			Subject:   creds.ClientID,
		},
	}

	// Create token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign token
	tokenString, err := token.SignedString([]byte(s.config.Secret))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not generate token"})
		return
	}

	// Return token
	c.JSON(http.StatusOK, gin.H{
		"token":      tokenString,
		"expires_at": expirationTime.Format(time.RFC3339),
		"client_id":  creds.ClientID,
	})
}

// ValidateToken validates a JWT token
func (s *Service) ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}

	// Parse token
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(s.config.Secret), nil
	})

	if err != nil {
		return nil, err
	}

	if !token.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}

// Middleware creates a Gin middleware for authentication
func (s *Service) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header is required"})
			return
		}

		// Check if it starts with "Bearer "
		const bearerPrefix = "Bearer "
		if len(authHeader) <= len(bearerPrefix) || authHeader[:len(bearerPrefix)] != bearerPrefix {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Authorization header must start with 'Bearer '",
			})
			return
		}

		// Extract token
		tokenString := authHeader[len(bearerPrefix):]

		// Validate token
		claims, err := s.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + err.Error()})
			return
		}

		// Set client ID in context
		c.Set(string(ClientIDKey), claims.ClientID)

		c.Next()
	}
}
