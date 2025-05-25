let ws = null;
const clientId = Math.random().toString(36).substring(7); // This is for WebSocket client_id, distinct from persistent userId
// let currentSessionId = null; // Replaced by currentChatTarget
let currentChatTarget = { type: null, id: null }; // type: 'session' or 'job', id: session_id or job_id
let knownSessions = {}; // Store as { id: "uuid", name: "Chat YYYY-MM-DD HH:MM", timestamp: date }
let persistentUserId = null;
let registered_agents_cache = {}; // Initialize agent cache

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
        initializeApp();
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
            // Display if it belongs to the current chat target (session or job)
            if (message.data && message.data.session_id === currentChatTarget.id) {
                displayChatMessage(message.data);
            } else {
                // console.log(`Chat message for another context (${message.data.session_id}, current: ${currentChatTarget.id}) received.`);
            }
            break;
        case 'agent_update':
            // console.log("UI: Received agent_update message (agent panel removed):", message.data);
            // Still update the cache for the job submission modal
            if (message.data && message.data.id) {
               registered_agents_cache[message.data.id] = message.data;
               console.log(`Agent cache updated for ${message.data.id} via WebSocket.`);
            }
            break;
    }
}

// Load initial data
async function loadInitialData() {
    // Load agents for the modal, not for display
    try {
        const agentsResponse = await fetch('/api/agents');
        if (agentsResponse.ok) {
            const agents = await agentsResponse.json();
            agents.forEach(agent => { // Populate cache for job submission modal
                if (agent && agent.id) {
                    registered_agents_cache[agent.id] = agent;
                }
            });
            console.log("Agents loaded into cache for modal.");
        } else {
            console.error("Failed to load agents for modal cache:", agentsResponse.status, await agentsResponse.text());
        }
    } catch (error) {
        console.error("Error fetching agents for modal cache:", error);
    }
        
    // Load recent jobs
    try {
        const jobsResponse = await fetch('/api/jobs'); // Path will be proxied
        if (jobsResponse.ok) {
            const jobs = await jobsResponse.json(); // Assuming server returns newest first
            // Reverse order for processing because updateJobDisplay prepends,
            // so processing oldest first will result in newest at the top.
            jobs.reverse().forEach(job => updateJobDisplay(job));
            console.log(`Loaded ${jobs.length} existing jobs.`);
        } else {
            console.error("Failed to load existing jobs:", jobsResponse.status, await jobsResponse.text());
        }
    } catch (error) {
        console.error("Error fetching existing jobs:", error);
    }
    
    // Chat history will be loaded by switchSession or createNewSession
}

async function initializeApp() {
    persistentUserId = getOrSetUserId();
    document.getElementById('user-info').textContent = `User ID: ${persistentUserId}`;

    loadInitialData(); // For agents and jobs
    await loadSessionsFromServer(); // Load sessions from server first

    // If no sessions after server load, create one. Otherwise, select one.
    // Prioritize last active target (could be session or job)
    const lastActiveTarget = JSON.parse(localStorage.getItem('currentChatTarget'));

    if (lastActiveTarget && lastActiveTarget.id) {
        if (lastActiveTarget.type === 'session' && knownSessions[lastActiveTarget.id]) {
            await switchSession(lastActiveTarget.id);
        } else if (lastActiveTarget.type === 'job') {
            // We need to ensure the job exists in the jobs list if we want to focus it.
            // For now, let's assume jobs are loaded/updated via WebSocket.
            // If the job is known (e.g. from a previous job_update), focus it.
            // This part might need refinement if jobs aren't persistently loaded like sessions.
            const jobElement = document.getElementById(`job-${lastActiveTarget.id}`);
            if (jobElement) { // A simple check if the job is rendered
                 await focusJob(lastActiveTarget.id);
            } else {
                await selectDefaultSessionOrJob();
            }
        } else {
            await selectDefaultSessionOrJob();
        }
    } else {
        await selectDefaultSessionOrJob();
    }
}

async function selectDefaultSessionOrJob() {
    if (Object.keys(knownSessions).length > 0) {
        const sortedSessions = Object.values(knownSessions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        await switchSession(sortedSessions[0].id);
    } else {
        // If no sessions, and potentially no jobs to default to, create a new session.
        await createNewSession();
    }
    // If there are jobs but no sessions, one could implement logic to focus a default job.
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


function saveCurrentChatTargetToLocalStorage() {
    localStorage.setItem('currentChatTarget', JSON.stringify(currentChatTarget));
}

// function loadSessionsFromLocalStorage() { // This is now more of a fallback or for currentSessionId
//     const storedSessions = localStorage.getItem('knownSessions');
//     if (storedSessions) {
//         // This might be overwritten by server load, which is intended.
//         // knownSessions = JSON.parse(storedSessions); 
//     }
//     // currentSessionId is still useful to remember the last active tab.
// }

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
            // saveSessionsToLocalStorage(); // currentChatTarget will be saved by switchSession
            renderSessionList();
            await switchSession(session.id); // This will set currentChatTarget and save it
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

    currentChatTarget = { type: 'session', id: sessionId };
    document.getElementById('chat-messages').innerHTML = ''; // Clear previous messages
    document.getElementById('chat-title').textContent = `Chat: ${knownSessions[sessionId].name}`;

    // Highlight active session and deactivate any active job
    updateActiveSessionHighlight(sessionId);
    updateActiveJobHighlight(null);
    
    saveCurrentChatTargetToLocalStorage();
    await loadChatHistory(sessionId, 'session');
}

async function focusJob(jobId) {
    const jobElement = document.getElementById(`job-${jobId}`);
    if (!jobElement) {
        console.error(`Job element for ${jobId} not found.`);
        // Potentially switch to a default session if current job focus is invalid
        await selectDefaultSessionOrJob();
        return;
    }

    currentChatTarget = { type: 'job', id: jobId };
    document.getElementById('chat-messages').innerHTML = ''; // Clear previous messages
    // Extract job description or use ID for title
    const jobDescElement = jobElement.querySelector('.job-description');
    const jobTitleName = jobDescElement ? jobDescElement.textContent.substring(0,30) + "..." : jobId.substring(0,8);
    document.getElementById('chat-title').textContent = `Job: ${jobTitleName}`;

    // Highlight active job and deactivate any active session
    updateActiveJobHighlight(jobId);
    updateActiveSessionHighlight(null);

    saveCurrentChatTargetToLocalStorage();
    await loadChatHistory(jobId, 'job'); // Use 'job' type to potentially fetch from a different conceptual endpoint if needed, though session_id is the key
}


async function loadChatHistory(targetId, targetType) {
    if (!targetId) {
        console.log("No target ID provided to loadChatHistory.");
        document.getElementById('chat-messages').innerHTML = '<div>Select a session or job to view chat.</div>';
        return;
    }
    // The session_id for chat history is always the targetId (be it a session_id or a job_id)
    const historySessionId = targetId; 
    try {
        const chatResponse = await fetch(`/api/chat/history/${historySessionId}`);
        if (chatResponse.ok) {
            const chatHistory = await chatResponse.json();
            chatHistory.messages.forEach(msg => displayChatMessage(msg));
        } else {
            console.error(`Failed to load chat history for ${targetType} ${targetId}:`, chatResponse.status, await chatResponse.text());
            document.getElementById('chat-messages').innerHTML = `<div>Error loading history for ${targetType} ${targetId}.</div>`;
        }
    } catch (error) {
        console.error(`Error fetching chat history for ${targetType} ${targetId}:`, error);
        document.getElementById('chat-messages').innerHTML = `<div>Could not fetch history for ${targetType} ${targetId}.</div>`;
    }
}

function renderSessionList() {
    const sessionsListElement = document.getElementById('sessions-list');
    sessionsListElement.innerHTML = '';

    const sortedSessions = Object.values(knownSessions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    sortedSessions.forEach(session => {
        const listItem = document.createElement('li');
        listItem.textContent = session.name;
        listItem.dataset.sessionId = session.id;
        if (currentChatTarget.type === 'session' && session.id === currentChatTarget.id) {
            listItem.classList.add('active-session');
        }
        listItem.onclick = () => switchSession(session.id);
        sessionsListElement.appendChild(listItem);
    });
}

function updateActiveSessionHighlight(activeSessionId) {
    const sessionListItems = document.querySelectorAll('#sessions-list li');
    sessionListItems.forEach(item => {
        item.classList.remove('active-session');
        if (item.dataset.sessionId === activeSessionId) {
            item.classList.add('active-session');
        }
    });
}

function updateActiveJobHighlight(activeJobId) {
    const jobListItems = document.querySelectorAll('#jobs-list .job-item'); // Assuming jobs are list items or divs with .job-item
    jobListItems.forEach(item => {
        item.classList.remove('active-job');
        if (item.id === `job-${activeJobId}`) { // Assuming job items have id `job-${job.id}`
            item.classList.add('active-job');
        }
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
    const messageContent = input.value.trim();
    
    if (!currentChatTarget.id) {
        alert("Please select a session or job to chat with.");
        return;
    }

    if (messageContent && ws.readyState === WebSocket.OPEN) {
        const messagePayload = {
            type: 'chat', // All messages to coordinator are 'chat' type for now
                          // Coordinator will then publish to 'chat-messages' topic
            session_id: currentChatTarget.id, // This ID is the key for chat history (session_id or job_id)
            content: messageContent
        };

        // If the target is a job, we might want to add specific metadata,
        // but for now, using job_id as session_id is the main mechanism.
        // if (currentChatTarget.type === 'job') {
        //    messagePayload.job_id = currentChatTarget.id; // Could be redundant if session_id is job_id
        // }

        ws.send(JSON.stringify(messagePayload));
        input.value = '';
    }
}

// Update displays
function updateJobDisplay(job) {
    const jobsList = document.getElementById('jobs-list'); // Should be a <ul>
    let jobElement = document.getElementById(`job-${job.id}`);
    
    if (!jobElement) {
        jobElement = document.createElement('li'); // Changed to li
        jobElement.id = `job-${job.id}`;
        jobElement.className = 'job-item';
        jobsList.prepend(jobElement); // Add to the top of the list
        jobElement.onclick = () => focusJob(job.id); // Make job item clickable
    }
    
    // Highlight if it's the current chat target
    if (currentChatTarget.type === 'job' && currentChatTarget.id === job.id) {
        jobElement.classList.add('active-job');
    } else {
        jobElement.classList.remove('active-job');
    }
    
    const description = job.payload && job.payload.description ? job.payload.description : 'No description';
    jobElement.innerHTML = `
        <div>
            <strong>Job ${job.id.substring(0, 8)}</strong>
            <span class="status ${job.status}">${job.status}</span>
        </div>
        <div class="job-description">Desc: ${description.substring(0,50)}${description.length > 50 ? '...' : ''}</div>
        <div>Type: ${job.task_type}</div>
        ${job.agent_id ? `<div>Agent: ${job.agent_id.substring(0,8)}</div>` : '<div>Agent: Any</div>'}
        ${job.result ? `<div>Result: ${JSON.stringify(job.result).substring(0,50)}...</div>` : ''}
    `;
}

// function updateAgentDisplay(agent) { // Function removed as panel is gone
//     // Logic for updating registered_agents_cache is still needed if modal uses it.
//     // This is now handled in loadInitialData and potentially in ws handler for 'agent_update'
//     // if live updates to the modal's agent list are desired without a visible panel.
// }

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
