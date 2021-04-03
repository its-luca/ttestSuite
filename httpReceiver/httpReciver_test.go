package httpReceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

//Test if receiver shuts down on context expiry
func Test_Receiver_Cancel(t *testing.T) {
	srv := httptest.NewUnstartedServer(nil)
	rec := NewReceiver()
	srv.Config.Handler = rec.Routes()
	srv.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_, updateChan := rec.WaitForMeasureStart(ctx)

		abortText, cancelAbortTest := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			defer cancelAbortTest()
			for {
				select {
				case _, ok := <-updateChan:
					{
						if ok {
							t.Errorf("channel was not closed as expected")
						}
						return //go
					}
				case <-abortText.Done():
					{
						t.Errorf("channel was not closed in expected time frame\n")
						return
					}
				}

			}
		}()
	}()

	cancel()

}

func Test_Receiver_CancelAfterReceivedAll(t *testing.T) {
	srv := httptest.NewUnstartedServer(nil)
	rec := NewReceiver()
	srv.Config.Handler = rec.Routes()
	srv.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wantTotalTraceFileCount := 5
	wantUpdateFileName := "dummyFile"
	startPayload, err := json.Marshal(&measureStartMsg{TotalNumberOfTraceFiles: wantTotalTraceFileCount})
	if err != nil {
		t.Fatalf("Failed to prepare start payload :%v\n", err)
	}
	updatePayload, err := json.Marshal(&measureUpdateMsg{FileName: wantUpdateFileName})
	if err != nil {
		t.Fatalf("Failed to prepare update payload : %v\n", err)
	}
	go func() {
		rs, err := srv.Client().Post(srv.URL+"/start", "application/json", bytes.NewReader(startPayload))
		if err != nil {
			t.Fatalf("Unexpected post error for measure start msg :%v\n", err)
		}
		if rs.StatusCode != http.StatusOK {
			t.Fatalf("unexpected error status for measure start msg :%v\n", rs.Status)
		}
	}()

	gotTotalTraceFileCount, updateChannel := rec.WaitForMeasureStart(ctx)
	if gotTotalTraceFileCount != wantTotalTraceFileCount {
		t.Errorf("want totalTraceFileCount %v, got %v", wantTotalTraceFileCount, gotTotalTraceFileCount)
	}

	for i := 0; i < wantTotalTraceFileCount; i++ {
		rs, err := srv.Client().Post(srv.URL+"/newFile", "application/json", bytes.NewReader(updatePayload))
		if err != nil {
			t.Fatalf("Unexpected post error for measure udpate msg:%v\n", err)
		}
		if rs.StatusCode != http.StatusOK {
			t.Fatalf("unexpected error status for measure udpate msg:%v\n", rs.Status)
		}
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 3*time.Second)
		select {
		case gotFileName := <-updateChannel:
			if gotFileName != wantUpdateFileName {
				t.Errorf("got update filename :%v, want %v", gotFileName, wantUpdateFileName)
			}
		case <-abortCtx.Done():
			t.Fatalf("waiting for next update filename timed out\n")
		}
		abortCancel()
	}

	//check receiver closed channel
	abortCtx, abortCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer abortCancel()
	select {
	case _, ok := <-updateChannel:
		if ok {
			t.Errorf("channel was not closed as expected")
		}
	case <-abortCtx.Done():
		t.Fatalf("waiting for channel to be closed timed out\n")
	}

}
