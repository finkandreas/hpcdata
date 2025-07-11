package handler

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v2"
	"github.com/golang-jwt/jwt/v5"

	"cscs.ch/hpcdata/logging"
)

var jwks_keyfunc *keyfunc.JWKS = nil
var jwks_keyfunc_api_gw *keyfunc.JWKS = nil

func PrepareJwksKeyfunc(url string) {
	jwks_keyfunc = prepareJwksKeyfuncHelper(url)
}
func PrepareJwksKeyfuncApiGw(url string) {
	jwks_keyfunc_api_gw = prepareJwksKeyfuncHelper(url)
}
func prepareJwksKeyfuncHelper(url string) *keyfunc.JWKS {
	options := keyfunc.Options{
		RefreshInterval:   time.Hour * 24,
		RefreshRateLimit:  time.Minute * 1,
		RefreshUnknownKID: true,
	}

	if ret, err := keyfunc.Get(url, options); err != nil {
		panic(fmt.Sprintf("Error while fetching JWKS certificates. err=%v", err))
	} else {
		return ret
	}
}

func validate_jwt(r *http.Request) ([]string, error) {
	token := strings.TrimSpace(strings.Replace(r.Header.Get("Authorization"), "Bearer", "", 1))
	var ret []string
	var access_token *jwt.Token
	var err error

	// try all possible JWKS keyfuncs
	for _, keyfunc := range []*keyfunc.JWKS{jwks_keyfunc, jwks_keyfunc_api_gw} {
		if access_token, err = jwt.Parse(token, keyfunc.Keyfunc); err == nil {
			break // found a working keyfunc
		}
	}

	// all JWT parsing failed, return the error of the last failed parsing
	if err != nil {
		return nil, err
	} else {
		mapclaims := access_token.Claims.(jwt.MapClaims)
		if scope, exists := mapclaims["scope"]; exists == false {
			logging.Errorf(condition_error{"JWT claims did not include scope"}, "JWT claims did not include scope. All claims=%#v", mapclaims)
		} else {
			ret = strings.Split(scope.(string), " ")
		}
	}
	return ret, nil
}
