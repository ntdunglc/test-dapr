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
    loadSessionsFromLocalStorage();
    renderSessionList();

    if (Object.keys(knownSessions).length === 0) {
        await createNewSession(); // Create a default session if none exist
    } else {
        // Try to load the last active session, or the most recent one
        const lastActiveId = localStorage.getItem('currentSessionId');
        if (lastActiveId && knownSessions[lastActiveId]) {
            await switchSession(lastActiveId);
        } else {
            // Fallback to the most recent session if last active is not found or invalid
            const sortedSessions = Object.values(knownSessions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            if (sortedSessions.length > 0) {
                await switchSession(sortedSessions[0].id);
            } else {
                 await createNewSession(); // Should not happen if previous block created one
            }
        }
    }
}

function saveSessionsToLocalStorage() {
    localStorage.setItem('knownSessions', JSON.stringify(knownSessions));
    if (currentSessionId) {
        localStorage.setItem('currentSessionId', currentSessionId);
    }
}

function loadSessionsFromLocalStorage() {
    const storedSessions = localStorage.getItem('knownSessions');
    if (storedSessions) {
        knownSessions = JSON.parse(storedSessions);
    }
    // currentSessionId will be loaded and set by initializeApp logic
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


// Submit a new job
async function submitJob() {
    const taskTypes = ['data_processing', 'analysis'];
    const taskType = taskTypes[Math.floor(Math.random() * taskTypes.length)];
    
    const payload = {
        item_count: Math.floor(Math.random() * 1000) + 100,
        priority: Math.random() > 0.5 ? 'high' : 'normal'
    };
    
    const response = await fetch('/jobs/submit', { // Corrected path
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            task_type: taskType,
            payload: payload
        })
    });
    
    const result = await response.json();
    console.log('Job submitted:', result.job_id);
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
        ${job.agent_id ? `<div>Agent: ${job.agent_id}</div>` : ''}
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
    
    agentElement.innerHTML = `
        <div>
            <strong>${agent.name}</strong>
            <span class="status ${agent.status}">${agent.status}</span>
        </div>
        <div>Type: ${agent.type}</div>
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
