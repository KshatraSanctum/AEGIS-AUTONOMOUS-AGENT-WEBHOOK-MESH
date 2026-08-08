package handlers

import (
	"html/template"
	"net/http"
	"zerops-webhook-mesh/internal/storage"
)

type DashboardHandler struct {
	Store *storage.PostgresStore
}

func NewDashboardHandler(store *storage.PostgresStore) *DashboardHandler {
	return &DashboardHandler{Store: store}
}

type Event struct {
	EventID, SourceID, Status, CreatedAt string
}

type DLQEvent struct {
	EventID, SourceID, ErrorReason, FailedAt string
}

func (h *DashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var events []Event
	var dlqEvents []DLQEvent

	// If Store is initialized (production or docker mode), fetch real database rows.
	// If Store is nil (local test mode), gracefully default to empty slices to prevent panics.
	if h.Store != nil && h.Store.DB != nil {
		rows, err := h.Store.DB.Query("SELECT event_id, source_id, status, created_at FROM webhook_events ORDER BY created_at DESC LIMIT 6")
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var e Event
				rows.Scan(&e.EventID, &e.SourceID, &e.Status, &e.CreatedAt)
				events = append(events, e)
			}
		}

		dlqRows, err := h.Store.DB.Query("SELECT event_id, source_id, error_reason, failed_at FROM webhook_dlq ORDER BY failed_at DESC LIMIT 6")
		if err == nil {
			defer dlqRows.Close()
			for dlqRows.Next() {
				var d DLQEvent
				dlqRows.Scan(&d.EventID, &d.SourceID, &d.ErrorReason, &d.FailedAt)
				dlqEvents = append(dlqEvents, d)
			}
		}
	}

	data := struct {
		Events    []Event
		DLQEvents []DLQEvent
	}{Events: events, DLQEvents: dlqEvents}

	html := `
    <!DOCTYPE html>
    <html lang="en" class="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Aegis | SRE Mesh</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
            body { font-family: 'Inter', sans-serif; background-color: #09090b; color: #fafafa; }
            .glass-panel { background: rgba(24, 24, 27, 0.6); border: 1px solid #27272a; backdrop-filter: blur(12px); }
        </style>
    </head>
    <body class="p-8 antialiased">
        <div class="max-w-6xl mx-auto space-y-8">
            <!-- Header -->
            <div class="glass-panel rounded-xl p-6 shadow-2xl flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-semibold tracking-tight text-white flex items-center gap-2">
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-indigo-500"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                        Aegis Control Plane
                    </h1>
                    <p class="text-sm text-zinc-400 mt-1">Autonomous Resilience for AI Agent Swarms</p>
                </div>
                <div class="flex items-center gap-4">
                    <div class="flex items-center gap-2 px-3 py-1 rounded-full border border-emerald-900/50 bg-emerald-950/30 text-emerald-400 text-xs font-medium tracking-wide">
                        <span class="relative flex h-2 w-2"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span><span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span></span>
                        SYSTEM OPERATIONAL
                    </div>
                    <button onclick="triggerSimulation()" class="bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-medium py-2 px-4 rounded-md transition shadow-lg shadow-indigo-900/20">
                        Simulate AI Outage
                    </button>
                </div>
            </div>
            
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- Ingress Stream -->
                <div class="space-y-3">
                    <h2 class="text-sm font-medium text-zinc-400 uppercase tracking-wider flex items-center gap-2">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
                        Ingress Traffic
                    </h2>
                    <div class="glass-panel rounded-lg overflow-hidden">
                        <table class="w-full text-left text-sm">
                            <tbody id="active-table" class="divide-y divide-zinc-800/50">
                                {{range .Events}}
                                <tr class="hover:bg-zinc-800/20 transition-colors">
                                    <td class="p-4 font-mono text-xs text-zinc-300">{{.EventID}}</td>
                                    <td class="p-4 text-zinc-400">{{.SourceID}}</td>
                                    <td class="p-4 text-right"><span class="px-2 py-1 text-[10px] uppercase font-bold rounded bg-emerald-950/50 text-emerald-400 border border-emerald-900/50">{{.Status}}</span></td>
                                </tr>
                                {{else}}
                                <tr><td colspan="3" class="p-8 text-center text-zinc-500 text-sm">Awaiting ingress traffic...</td></tr>
                                {{end}}
                            </tbody>
                        </table>
                    </div>
                </div>

                <!-- DLQ -->
                <div class="space-y-3">
                    <h2 class="text-sm font-medium text-zinc-400 uppercase tracking-wider flex items-center gap-2">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-red-400"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                        Dead Letter Queue
                    </h2>
                    <div class="glass-panel rounded-lg overflow-hidden border-red-900/20">
                        <table class="w-full text-left text-sm">
                            <tbody id="dlq-table" class="divide-y divide-zinc-800/50">
                                {{range .DLQEvents}}
                                <tr class="hover:bg-zinc-800/20 transition-colors">
                                    <td class="p-4 font-mono text-xs text-red-400/80">{{.EventID}}</td>
                                    <td class="p-4 text-zinc-400 text-xs truncate max-w-[150px]">{{.ErrorReason}}</td>
                                    <td class="p-4 text-right">
                                        <button onclick="replayEvent('{{.EventID}}')" class="bg-zinc-800 hover:bg-zinc-700 text-zinc-300 px-3 py-1.5 rounded text-xs border border-zinc-700 transition flex items-center gap-2 ml-auto">
                                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"/></svg>
                                            Replay
                                        </button>
                                    </td>
                                </tr>
                                {{else}}
                                <tr><td colspan="3" class="p-8 text-center text-zinc-500 text-sm">No cascading failures detected.</td></tr>
                                {{end}}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <script>
            async function fetchUpdates() {
                try {
                    let res = await fetch(window.location.href);
                    if (res.status === 401) { window.location.reload(); return; }
                    let text = await res.text();
                    let doc = new DOMParser().parseFromString(text, 'text/html');
                    document.getElementById('active-table').innerHTML = doc.getElementById('active-table').innerHTML;
                    document.getElementById('dlq-table').innerHTML = doc.getElementById('dlq-table').innerHTML;
                } catch (e) {}
            }
            setInterval(fetchUpdates, 1500); 

            async function triggerSimulation() {
                await fetch('/v1/simulate', { method: 'POST' });
            }

            async function replayEvent(eventId) {
                await fetch('/v1/replay?event_id=' + eventId, { method: 'POST' });
                fetchUpdates(); 
            }
        </script>
    </body>
    </html>`

	tmpl, _ := template.New("dashboard").Parse(html)
	tmpl.Execute(w, data)
}