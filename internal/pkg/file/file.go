// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package file

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/model"
)

const (
	// specification-designated maximum
	MaxChunkSize = 4194304 // 4 MiB
)

var (
	ErrInvalidID = errors.New("file not found with this ID, it may have been removed")
)

// Status represents the only valid values of upload status according to storage spec
type Status string

const (
	StatusAwaiting Status = "AWAITING_UPLOAD"
	StatusProgress Status = "UPLOADING"
	StatusDone     Status = "READY"
	StatusFail     Status = "UPLOAD_ERROR"
	StatusDel      Status = "DELETED"
)

type FileData struct {
	Size      int64  `json:"size"`
	ChunkSize int64  `json:"ChunkSize"`
	Status    string `json:"Status"`
	MimeType  string `json:"mime_type,omitempty"`
	Hash      *Hash  `json:"hash,omitempty"`
}

type Hash struct {
	SHA2 string `json:"sha256,omitempty"`
}

type MetaDoc struct {
	ActionID    string    `json:"action_id"`
	AgentID     string    `json:"agent_id"`
	Source      string    `json:"src"`
	File        FileData  `json:"file"`
	UploadID    string    `json:"upload_id"`
	Start       time.Time `json:"upload_start"`
	End         time.Time `json:"upload_end,omitempty"`
	Namespaces  []string  `json:"namespaces"`
	TransitHash *Hash     `json:"transithash,omitempty"`
}

// custom unmarshaller to make unix-epoch values work
func (m *MetaDoc) UnmarshalJSON(b []byte) error {
	type InnerFile MetaDoc // type alias to prevent recursion into this func
	// override the fields to parse as an int, then manually convert to time.time
	var tmp struct {
		InnerFile
		Start int64 `json:"upload_start"`
		End   int64 `json:"upload_end"`
	}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	*m = MetaDoc(tmp.InnerFile) // copy over all fields
	m.Start = time.UnixMilli(tmp.Start)
	if tmp.End != 0 {
		m.End = time.UnixMilli(tmp.End)
	}
	return nil
}

type ChunkInfo struct {
	Pos        int    // Ordered chunk position in file
	Last       bool   // Is this the final chunk in the file
	SHA2       string // Content hash of this chunk
	UploadID   string // Upload session generation this chunk belongs to
	Size       int
	BID        string // base id, matches metadata doc's _id
	Index      string
	ID         string // chunk _id
	Namespaces []string
}

type Info struct {
	ID          string // upload operation identifier, also the session generation. Used to identify the upload process
	DocID       string // document ID of the uploaded file and chunks
	Source      string // which integration is performing the upload
	AgentID     string
	ActionID    string
	Namespaces  []string
	ChunkSize   int64
	Total       int64
	Count       int
	Start       time.Time
	Status      Status
	TransitHash string // transit (streaming) hash recorded when the upload was published
}

// convenience functions for computing current "Status" based on the fields
func (i Info) Expired(timeout time.Duration) bool { return time.Now().After(i.Start.Add(timeout)) }

// Published is true once the file's metadata has been atomically flipped to
// READY. Only published files may be served whole or via Range requests.
func (i Info) Published() bool { return i.Status == StatusDone }

// StatusCanUpload returns true while chunks may still be written for the session.
// It returns false once the upload failed or was deleted.
func (i Info) StatusCanUpload() bool {
	return i.Status != StatusFail && i.Status != StatusDel
}

// StatusCanUploadChunks returns true if a chunk PUT may be accepted.
// Chunk writes are idempotent (position + content hash), so retries remain
// accepted even after the file has been published: a conflicting create cannot
// overwrite a confirmed chunk, identical content simply succeeds again.
// Expired, unpublished sessions and failed/deleted sessions are closed.
func (i Info) StatusCanUploadChunks(timeout time.Duration) bool {
	if i.Status == StatusFail || i.Status == StatusDel {
		return false
	}
	if i.Status == StatusDone {
		return true
	}
	return !i.Expired(timeout)
}

type Chunk struct {
	model.ESDocument

	BID  string `json:"bid"`
	Data []byte `json:"data"`
	Last bool   `json:"last"`
	SHA2 string `json:"sha2"`
}
