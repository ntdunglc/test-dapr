let ws = null;
const clientId = Math.random().toString(36).substring(7); // This is for WebSocket client_id, distinct from persistent userId
let currentSessionId = null;
let knownSessions = {}; // Store as { id: "uuid", name: "Chat YYYY-MM-DD HH:MM", timestamp: date }
let persistentUserId = null;

function getOrSetUserId() {
    let userId = localStorage.getItem('persistentUserId');
    if (!userId) {
        userId = `user-${Math.random().toString(36).substring(2, 9)}`; // Generate a simple user ID
        localStorage.setItem('persistentUserId', userId);
    }
    return userId;
}

// Initialize WebSocket connection
function initWebSocket() {
    ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`);
    
    ws.onopen = () => {
        console.log('Connected to coordinator');
        initializeApp(); // Changed from loadInitialData to a more comprehensive init
    };
    
    ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        handleMessage(message);
    };
    
    ws.onclose = () => {
        console.log('Disconnected from coordinator');
        setTimeout(initWebSocket, 3000); // Reconnect after 3 seconds
    };
}

// Handle incoming messages
function handleMessage(message) {
    switch(message.type) {
        case 'job_update':
            updateJobDisplay(message.data);
            break;
        case 'chat_message':
            // Only display if it belongs to the current session
            if (message.data && message.data.session_id === currentSessionId) {
                displayChatMessage(message.data);
            } else {
                // Optionally, notify if a message for another session arrives
                // console.log(`Chat message for another session (${message.data.session_id}) received.`);
                // Or update a badge on the session list item
            }
            break;
        case 'agent_update':
            console.log("UI: Received agent_update message:", message.data); // Added log
            updateAgentDisplay(message.data);
            break;
    }
}

// Load initial data
async function loadInitialData() {
    // Load agents
    try {
        const agentsResponse = await fetch('/api/agents'); // Path will be proxied by web-ui/server.py
        if (agentsResponse.ok) {
            const agents = await agentsResponse.json();
            agents.forEach(agent => updateAgentDisplay(agent));
        } else {
            console.error("Failed to load agents:", agentsResponse.status, await agentsResponse.text());
        }
    } catch (error) {
        console.error("Error fetching agents:", error);
    }
    
    // Load recent jobs
    // const jobsResponse = await fetch('/api/jobs/recent'); // Endpoint not yet implemented
    // const jobs = await jobsResponse.json();
    // jobs.forEach(job => updateJobDisplay(job));
    
    // Chat history will be loaded by switchSession or createNewSession
}

async function initializeApp() {
    persistentUserId = getOrSetUserId();
    document.getElementById('user-info').textContent = `User ID: ${persistentUserId}`;

    loadInitialData(); // For agents and jobs
    await loadSessionsFromServer(); // Load sessions from server first

    // If no sessions after server load, create one. Otherwise, select one.
    if (Object.keys(knownSessions).length === 0) {
        await createNewSession(); 
    } else {
        const lastActiveId = localStorage.getItem('currentSessionId');
        if (lastActiveId && knownSessions[lastActiveId]) {
            await switchSession(lastActiveId);
        } else {
            const sortedSessions = Object.values(knownSessions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            if (sortedSessions.length > 0) {
                await switchSession(sortedSessions[0].id);
            } 
            // If still no session (e.g. localStorage had an ID for a now-deleted session), createNewSession would have been called.
        }
    }
}

async function loadSessionsFromServer() {
    console.log("Loading sessions from server...");
    try {
        const response = await fetch('/api/sessions');
        if (response.ok) {
            const serverSessions = await response.json();
            knownSessions = {}; // Reset local cache with server data as source of truth
            serverSessions.forEach(session => {
                // Generate a client-side friendly name if not provided by server, or use server's if available
                const sessionName = `Chat ${new Date(session.created_at).toLocaleDateString()} ${new Date(session.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
                knownSessions[session.id] = {
                    id: session.id,
                    name: sessionName, // Or use session.name if server provided it
                    timestamp: session.created_at,
                    user_id: session.user_id
                };
            });
            console.log(`Loaded ${Object.keys(knownSessions).length} sessions from server.`);
        } else {
            console.error("Failed to load sessions from server:", response.status, await response.text());
            // Fallback to local storage if server fetch fails? Or just start fresh?
            // For now, if server fails, knownSessions might be empty or retain previous local state.
            // Let's clear it to reflect server failure, then createNewSession will trigger if empty.
            knownSessions = {};
        }
    } catch (error) {
        console.error("Error fetching sessions from server:", error);
        knownSessions = {}; // Clear on error
    }
    renderSessionList(); // Update UI based on fetched/cleared sessions
    saveSessionsToLocalStorage(); // Persist the server-fetched (or cleared) list
}


function saveSessionsToLocalStorage() {
    localStorage.setItem('knownSessions', JSON.stringify(knownSessions));
    if (currentSessionId) {
        localStorage.setItem('currentSessionId', currentSessionId);
    }
}

function loadSessionsFromLocalStorage() { // This is now more of a fallback or for currentSessionId
    const storedSessions = localStorage.getItem('knownSessions');
    if (storedSessions) {
        // This might be overwritten by server load, which is intended.
        // knownSessions = JSON.parse(storedSessions); 
    }
    // currentSessionId is still useful to remember the last active tab.
}

async function createNewSession() {
    try {
        const response = await fetch('/sessions/create', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ user_id: persistentUserId }) // Send persistentUserId
        });
        if (response.ok) {
            const session = await response.json();
            const sessionName = `Chat ${new Date(session.created_at).toLocaleDateString()} ${new Date(session.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
            knownSessions[session.id] = { 
                id: session.id, 
                name: sessionName, 
                timestamp: session.created_at,
                user_id: session.user_id // Store user_id with session if needed for display
            };
            saveSessionsToLocalStorage();
            renderSessionList();
            await switchSession(session.id);
            return session.id;
        } else {
            console.error("Failed to create new session:", response.status, await response.text());
        }
    } catch (error) {
        console.error("Error creating new session:", error);
    }
    return null;
}

async function switchSession(sessionId) {
    if (!knownSessions[sessionId]) {
        console.error(`Session ${sessionId} not found in knownSessions.`);
        // Potentially create a new session or switch to a default if current is invalid
        if (Object.keys(knownSessions).length > 0) {
            const firstSessionId = Object.keys(knownSessions)[0];
            console.warn(`Switching to first available session: ${firstSessionId}`);
            await switchSession(firstSessionId);
        } else {
            await createNewSession();
        }
        return;
    }

    currentSessionId = sessionId;
    document.getElementById('chat-messages').innerHTML = ''; // Clear previous messages
    document.getElementById('chat-title').textContent = `Agent Chat (${knownSessions[sessionId].name})`;


    // Highlight active session in the list
    const sessionListItems = document.querySelectorAll('#sessions-list li');
    sessionListItems.forEach(item => {
        item.classList.remove('active-session');
        if (item.dataset.sessionId === sessionId) {
            item.classList.add('active-session');
        }
    });
    
    saveSessionsToLocalStorage(); // Save current session as active
    await loadChatHistory(sessionId);
}

async function loadChatHistory(sessionId) {
    if (!sessionId) {
        console.log("No session ID provided to loadChatHistory.");
        document.getElementById('chat-messages').innerHTML = '<div>Select or create a chat session.</div>';
        return;
    }
    try {
        const chatResponse = await fetch(`/api/chat/history/${sessionId}`);
        if (chatResponse.ok) {
            const chatHistory = await chatResponse.json();
            chatHistory.messages.forEach(msg => displayChatMessage(msg));
        } else {
            console.error(`Failed to load chat history for session ${sessionId}:`, chatResponse.status, await chatResponse.text());
            document.getElementById('chat-messages').innerHTML = `<div>Error loading history for session ${sessionId}.</div>`;
        }
    } catch (error) {
        console.error(`Error fetching chat history for session ${sessionId}:`, error);
        document.getElementById('chat-messages').innerHTML = `<div>Could not fetch history for session ${sessionId}.</div>`;
    }
}

function renderSessionList() {
    const sessionsListElement = document.getElementById('sessions-list');
    sessionsListElement.innerHTML = ''; // Clear existing list

    const sortedSessions = Object.values(knownSessions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    sortedSessions.forEach(session => {
        const listItem = document.createElement('li');
        listItem.textContent = session.name;
        listItem.dataset.sessionId = session.id;
        if (session.id === currentSessionId) {
            listItem.classList.add('active-session');
        }
        listItem.onclick = () => switchSession(session.id);
        sessionsListElement.appendChild(listItem);
    });
}


// Job Submission Modal Functions
const jobModal = document.getElementById('job-modal');
const jobAgentSelect = document.getElementById('job-agent-select');
const jobDescriptionInput = document.getElementById('job-description');

function openSubmitJobModal() {
    // Populate agent select
    jobAgentSelect.innerHTML = '<option value="">Any Agent</option>'; // Default option
    if (registered_agents_cache && Object.keys(registered_agents_cache).length > 0) {
        Object.values(registered_agents_cache).forEach(agent => {
            const option = document.createElement('option');
            option.value = agent.id;
            option.textContent = `${agent.name} (${agent.id.substring(0,8)})`;
            jobAgentSelect.appendChild(option);
        });
    } else {
        // Optionally, fetch agents if cache is empty, or disable specific agent selection
        console.log("No agents in cache to populate dropdown. User can only select 'Any Agent'.");
    }
    
    jobDescriptionInput.value = ''; // Clear previous description
    jobModal.style.display = 'block';
}

function closeSubmitJobModal() {
    jobModal.style.display = 'none';
}

async function handleModalJobSubmit() {
    const selectedAgentId = jobAgentSelect.value;
    const description = jobDescriptionInput.value.trim();

    if (!description) {
        alert('Please enter a job description.');
        return;
    }

    const jobData = new FormData();
    jobData.append('task_type', 'user_task'); // New task type for these kinds of jobs
    jobData.append('description', description);
    if (selectedAgentId) {
        jobData.append('agent_id', selectedAgentId);
    }
    // Note: No explicit 'payload' key here, description is top-level.
    // The coordinator will construct the payload.

    try {
        const response = await fetch('/jobs/submit', {
            method: 'POST',
            // Headers are not 'Content-Type': 'application/json' when using FormData
            // The browser will set the correct Content-Type for FormData (multipart/form-data)
            body: jobData
        });

        if (response.ok) {
            const result = await response.json();
            console.log('Job submitted from modal:', result.job_id);
            updateJobDisplay({ // Optimistically add/update job display
                id: result.job_id,
                agent_id: selectedAgentId || null,
                status: "pending", // Initial status
                task_type: "user_task",
                payload: { description: description },
                created_at: new Date().toISOString()
            });
        } else {
            console.error('Failed to submit job:', response.status, await response.text());
            alert(`Failed to submit job: ${await response.text()}`);
        }
    } catch (error) {
        console.error('Error submitting job:', error);
        alert(`Error submitting job: ${error.message}`);
    }

    closeSubmitJobModal();
}


// Send chat message
function sendMessage() {
    const input = document.getElementById('chat-input');
    const message = input.value.trim();
    
    if (!currentSessionId) {
        alert("Please select or create a chat session first.");
        return;
    }

    if (message && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'chat',
            session_id: currentSessionId, // Include current session ID
            content: message
        }));
        input.value = '';
    }
}

// Update displays
function updateJobDisplay(job) {
    const jobsList = document.getElementById('jobs-list');
    let jobElement = document.getElementById(`job-${job.id}`);
    
    if (!jobElement) {
        jobElement = document.createElement('div');
        jobElement.id = `job-${job.id}`;
        jobElement.className = 'job-item';
        jobsList.prepend(jobElement);
    }
    
    jobElement.innerHTML = `
        <div>
            <strong>Job ${job.id.substring(0, 8)}</strong>
            <span class="status ${job.status}">${job.status}</span>
        </div>
        <div>Type: ${job.task_type}</div>
        ${job.agent_id ? `<div>Agent: ${job.agent_id.substring(0,8)}</div>` : '<div>Agent: Any</div>'}
        ${job.payload && job.payload.description ? `<div>Desc: ${job.payload.description.substring(0,50)}${job.payload.description.length > 50 ? '...' : ''}</div>` : ''}
        ${job.result ? `<div>Result: ${JSON.stringify(job.result)}</div>` : ''}
    `;
}

function updateAgentDisplay(agent) {
    const agentsList = document.getElementById('agents-list');
    let agentElement = document.getElementById(`agent-${agent.id}`);
    
    if (!agentElement) {
        agentElement = document.createElement('div');
        agentElement.id = `agent-${agent.id}`;
        agentElement.className = 'agent-item';
        agentsList.appendChild(agentElement);
    }
    
    let commandsHtml = '';
    if (agent.supported_commands && agent.supported_commands.length > 0) {
        commandsHtml = `<div>Supports: ${agent.supported_commands.join(', ')}</div>`;
    }

    agentElement.innerHTML = `
        <div>
            <strong>${agent.name}</strong>
            <span class="status ${agent.status}">${agent.status}</span>
        </div>
        <div>Type: ${agent.type}</div>
        ${commandsHtml}
    `;
}

function displayChatMessage(message) {
    const chatMessages = document.getElementById('chat-messages');
    const messageElement = document.createElement('div');
    messageElement.innerHTML = `
        <strong>${message.sender_id}:</strong> ${message.content}
        <span style="color: #999; font-size: 12px; margin-left: 10px;">
            ${new Date(message.timestamp).toLocaleTimeString()}
        </span>
    `;
    chatMessages.appendChild(messageElement);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Event listeners
document.getElementById('chat-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        sendMessage();
    }
});

document.getElementById('new-chat-button').addEventListener('click', createNewSession);

// Initialize
initWebSocket();
