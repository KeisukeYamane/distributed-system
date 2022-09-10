package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

/**
* ① リクエストを構造体へデコーディングする
* ② ①の構造体を使いログにレコードを保存する
* ③ ログがレコードを保存したオフセットを取得し、
*    その結果をエンコーディングしてレスポンスに書き込む
 */
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req) // json -> struct
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

/**
* ① リクエストを構造体へデコーディングする
* ② ①の構造体を使い該当のoffsetに位置するレコードを取得する
* ③ その結果をエンコーディングしてレスポンスに書き込む
 */
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()

	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods(http.MethodPost) // ログの書き込み
	r.HandleFunc("/", httpsrv.handleConsume).Methods(http.MethodGet)  // ログの読み出し

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
