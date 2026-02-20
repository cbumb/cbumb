// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubWatcher struct {
	markProcessedErr error
	markProcessed    bool
}

func (s *stubWatcher) Start(_ context.Context)                          {}
func (s *stubWatcher) Events() <-chan Event                             { return nil }
func (s *stubWatcher) Close(_ context.Context) error                    { return nil }
func (s *stubWatcher) MarkProcessed(_ context.Context, _ []byte) error {
	s.markProcessed = true
	return s.markProcessedErr
}

func TestHandleProcessingFailure(t *testing.T) {
	originalErr := errors.New("handler failed")

	tests := []struct {
		name                 string
		markProcessedOnError bool
		markProcessedErr     error
		wantErr              string
		wantMarked           bool
	}{
		{
			name:                 "MarkProcessedOnError=false returns original error",
			markProcessedOnError: false,
			wantErr:              "handler failed",
			wantMarked:           false,
		},
		{
			name:                 "MarkProcessedOnError=true returns original error after marking",
			markProcessedOnError: true,
			wantErr:              "handler failed",
			wantMarked:           true,
		},
		{
			name:                 "MarkProcessedOnError=true with mark failure returns mark error",
			markProcessedOnError: true,
			markProcessedErr:     errors.New("db down"),
			wantErr:              "failed to mark event as processed",
			wantMarked:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watcher := &stubWatcher{markProcessedErr: tt.markProcessedErr}
			proc := &DefaultEventProcessor{
				changeStreamWatcher: watcher,
				config: EventProcessorConfig{
					MarkProcessedOnError: tt.markProcessedOnError,
				},
			}

			err := proc.handleProcessingFailure(
				context.Background(), "test-event-id", originalErr, time.Now(),
			)

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if got := err.Error(); !contains(got, tt.wantErr) {
				t.Errorf("error = %q, want substring %q", got, tt.wantErr)
			}

			if watcher.markProcessed != tt.wantMarked {
				t.Errorf("markProcessed = %v, want %v", watcher.markProcessed, tt.wantMarked)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
