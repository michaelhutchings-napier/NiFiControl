package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPUserClientCreateUser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/tenants/users" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in UserEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.Identity != "CN=scraper" {
			t.Fatalf("identity = %q", in.Component.Identity)
		}
		_ = json.NewEncoder(w).Encode(UserEntity{ID: "user-1", Revision: Revision{Version: 1}, Component: UserComponent{ID: "user-1", Identity: in.Component.Identity}})
	}))
	defer server.Close()

	created, err := (HTTPUserClient{}).CreateUser(t.Context(), server.URL, UserEntity{Component: UserComponent{Identity: "CN=scraper"}})
	if err != nil {
		t.Fatal(err)
	}
	if UserEntityID(*created) != "user-1" || created.Component.Identity != "CN=scraper" {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPUserClientListUsers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nifi-api/tenants/users" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(UsersEntity{Users: []UserEntity{
			{ID: "u1", Component: UserComponent{ID: "u1", Identity: "CN=a"}},
			{ID: "u2", Component: UserComponent{ID: "u2", Identity: "CN=b"}},
		}})
	}))
	defer server.Close()

	users, err := (HTTPUserClient{}).ListUsers(t.Context(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users[1].Component.Identity != "CN=b" {
		t.Fatalf("users = %#v", users)
	}
}

func TestHTTPUserClientDeleteUserSendsVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/nifi-api/tenants/users/u1" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("version") != "5" {
			t.Fatalf("version = %q", r.URL.Query().Get("version"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPUserClient{}).DeleteUser(t.Context(), server.URL, "u1", 5); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPUserGroupClientCreateUserGroupSendsMembers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/tenants/user-groups" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in UserGroupEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.Identity != "editors" || len(in.Component.Users) != 2 || in.Component.Users[0].ID != "u1" {
			t.Fatalf("group payload = %#v", in.Component)
		}
		_ = json.NewEncoder(w).Encode(UserGroupEntity{ID: "g1", Revision: Revision{Version: 1}, Component: UserGroupComponent{ID: "g1", Identity: in.Component.Identity, Users: in.Component.Users}})
	}))
	defer server.Close()

	created, err := (HTTPUserGroupClient{}).CreateUserGroup(t.Context(), server.URL, UserGroupEntity{Component: UserGroupComponent{
		Identity: "editors",
		Users:    []TenantRef{{ID: "u1"}, {ID: "u2"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if UserGroupEntityID(*created) != "g1" {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPUserClientGetUserNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := (HTTPUserClient{}).GetUser(t.Context(), server.URL, "missing")
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}
}
