let ws = null;
const clientId = Math.random().toString(36).substring(7); // This is for WebSocket client_id, distinct from persistent userId
// let currentSessionId = null; // Replaced by currentChatTarget
let currentChatTarget = { type: null, id: null }; // type: 'session' or 'job', id: session_id or job_id
let knownInteractions = {}; // Unified store for chats and jobs. Keyed by ID.
                           // Each item: { id, type: 'chat'/'job', name, timestamp, originalData: {}, ...jobSpecificStatus }
let known_jobs_cache = {}; // Still useful for full job objects if needed, though originalData in knownInteractions might suffice.
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
    // Load agents for the modal
    try {
        const agentsResponse = await fetch('/api/agents');
        if (agentsResponse.ok) {
            const agents = await agentsResponse.json();
            agents.forEach(agent => { 
                if (agent && agent.id) {
                    registered_agents_cache[agent.id] = agent;
                }
            });
            console.log("Agents loaded into cache for job modal.");
        } else {
            console.error("Failed to load agents for job modal cache:", agentsResponse.status, await agentsResponse.text());
        }
    } catch (error) { // Added opening brace
        console.error("Error fetching agents for job modal cache:", error);
    }
        
    // Load existing jobs and add them as 'job' type interactions
    try {
        const jobsResponse = await fetch('/api/jobs'); 
        if (jobsResponse.ok) {
            const jobs = await jobsResponse.json(); // Server returns newest first
            jobs.forEach(job => {
                known_jobs_cache[job.id] = job; // Keep full job data accessible
                knownInteractions[job.id] = {
                    id: job.id,
                    type: 'job',
                    name: `Job: ${job.payload.description ? job.payload.description.substring(0, 20) + "..." : job.id.substring(0,8)}`,
                    timestamp: job.created_at,
                    originalData: job,
                    status: job.status // Store job status for display in list
                };
            });
            console.log(`Loaded ${jobs.length} existing jobs into interactions list.`);
        } else {
            console.error("Failed to load existing jobs:", jobsResponse.status, await jobsResponse.text());
        }
    } catch (error) {
        console.error("Error fetching existing jobs:", error);
    }
    
    // Chat history will be loaded by switchSession or createNewSession
} // Added missing closing brace for loadInitialData function

async function initializeApp() {
    persistentUserId = getOrSetUserId();
    document.getElementById('user-info').textContent = `User ID: ${persistentUserId}`;

    await loadInitialData(); // Loads agents for modal, and jobs into knownInteractions
    await loadChatSessionsFromServer(); // Load chat sessions into knownInteractions

    renderInteractionList(); // Initial render of the merged list

    // Prioritize last active target
    const lastActiveTarget = JSON.parse(localStorage.getItem('currentChatTarget'));

    if (lastActiveTarget && lastActiveTarget.id && knownInteractions[lastActiveTarget.id]) {
        await focusInteraction(lastActiveTarget.id, lastActiveTarget.type);
    } else if (Object.keys(knownInteractions).length > 0) {
        // Default to the most recent item in the merged list
        const sortedInteractions = Object.values(knownInteractions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        await focusInteraction(sortedInteractions[0].id, sortedInteractions[0].type);
    } else {
        // If no interactions at all, create a new chat session
        await createNewChatSession();
    }
}

// async function selectDefaultSessionOrJob() { // Replaced by logic in initializeApp
//     if (Object.keys(knownInteractions).length > 0) {
//         const sortedInteractions = Object.values(knownInteractions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
//         await focusInteraction(sortedInteractions[0].id, sortedInteractions[0].type);
//     } else {
//         await createNewChatSession();
//     }
// }


async function loadChatSessionsFromServer() {
    console.log("Loading chat sessions from server...");
    try {
        const response = await fetch('/api/sessions');
        if (response.ok) {
            const serverSessions = await response.json();
            serverSessions.forEach(session => {
                const sessionName = `Chat ${new Date(session.created_at).toLocaleDateString()} ${new Date(session.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
                knownInteractions[session.id] = {
                    id: session.id,
                    type: 'chat',
                    name: sessionName,
                    timestamp: session.created_at,
                    originalData: session,
                    user_id: session.user_id
                };
            });
            console.log(`Loaded ${serverSessions.length} chat sessions into interactions list.`);
        } else {
            console.error("Failed to load chat sessions from server:", response.status, await response.text());
        }
    } catch (error) {
        console.error("Error fetching chat sessions from server:", error);
    }
    // renderInteractionList(); // Called after all initial data is loaded
    // saveInteractionsToLocalStorage(); // Persist after all initial data
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

async function createNewChatSession() {
    try {
        const response = await fetch('/sessions/create', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                user_id: persistentUserId
                // agent_id: "worker-1" // Removed: No longer defaulting new chats to LLM worker
            })
        });
        if (response.ok) {
            const session = await response.json();
            const sessionName = `Chat ${new Date(session.created_at).toLocaleDateString()} ${new Date(session.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
            knownInteractions[session.id] = { 
                id: session.id,
                type: 'chat',
                name: sessionName, 
                timestamp: session.created_at,
                originalData: session,
                user_id: session.user_id
            };
            renderInteractionList();
            await focusInteraction(session.id, 'chat');
            return session.id;
        } else {
            console.error("Failed to create new chat session:", response.status, await response.text());
        }
    } catch (error) {
        console.error("Error creating new chat session:", error);
    }
    return null;
}

async function focusInteraction(interactionId, interactionType) {
    const interaction = knownInteractions[interactionId];
    if (!interaction) {
        console.error(`Interaction ${interactionId} (type: ${interactionType}) not found.`);
        // Fallback logic: try to focus the most recent interaction or create a new chat
        if (Object.keys(knownInteractions).length > 0) {
            const sortedInteractions = Object.values(knownInteractions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            if (sortedInteractions.length > 0) {
                await focusInteraction(sortedInteractions[0].id, sortedInteractions[0].type);
            }
        } else {
            await createNewChatSession();
        }
        return;
    }

    currentChatTarget = { type: interactionType, id: interactionId };
    document.getElementById('chat-messages').innerHTML = ''; // Clear previous messages
    document.getElementById('chat-title').textContent = `${interactionType === 'job' ? 'Job' : 'Chat'}: ${interaction.name}`;

    // Highlight active interaction in the list
    const listItems = document.querySelectorAll('#interactions-list li');
    listItems.forEach(item => {
        item.classList.remove('active-interaction', 'type-chat', 'type-job');
        if (item.dataset.interactionId === interactionId) {
            item.classList.add('active-interaction', `type-${interactionType}`);
        }
    });
    
    // If it's a job, display the original task description
    if (interactionType === 'job' && interaction.originalData && interaction.originalData.payload) {
        const jobDescription = interaction.originalData.payload.description;
        if (jobDescription) {
            const chatMessagesDiv = document.getElementById('chat-messages');
            const taskElement = document.createElement('div');
            taskElement.className = 'original-job-task';
            taskElement.innerHTML = `<strong>Original Task:</strong><p>${jobDescription.replace(/\n/g, '<br>')}</p>`;
            chatMessagesDiv.appendChild(taskElement);
        }
    }

    saveCurrentChatTargetToLocalStorage();
    await loadChatHistory(interactionId, interactionType); // interactionId is used as session_id for history
}


async function loadChatHistory(targetId, targetType) { // targetType is 'chat' or 'job'
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

function renderInteractionList() {
    const interactionsListElement = document.getElementById('interactions-list');
    interactionsListElement.innerHTML = ''; // Clear existing list

    const sortedInteractions = Object.values(knownInteractions).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    sortedInteractions.forEach(interaction => {
        const listItem = document.createElement('li');
        listItem.dataset.interactionId = interaction.id;
        listItem.dataset.interactionType = interaction.type;

        let contentHtml = '';
        if (interaction.type === 'job') {
            const job = interaction.originalData;
            const jobStatus = interaction.status || job.status || 'unknown';
            contentHtml = `
                <span class="item-type-job-icon">💼</span>
                ${interaction.name}
                <span class="job-status-indicator ${jobStatus}">${jobStatus}</span>
            `;
        } else { // 'chat'
            contentHtml = `<span class="item-type-chat-icon">💬</span> ${interaction.name}`;
        }
        listItem.innerHTML = contentHtml;
        
        if (currentChatTarget.id === interaction.id) {
            listItem.classList.add('active-interaction', `type-${interaction.type}`);
        }
        listItem.onclick = () => focusInteraction(interaction.id, interaction.type);
        interactionsListElement.appendChild(listItem);
    });
}

// function updateActiveSessionHighlight(activeSessionId) { // Merged into focusInteraction
// }

// function updateActiveJobHighlight(activeJobId) { // Merged into focusInteraction
// }


// Job Submission Modal Functions
const jobModal = document.getElementById('job-modal');
const jobAgentSelect = document.getElementById('job-agent-select');
const jobDescriptionInput = document.getElementById('job-description');

function openSubmitJobModal() {
    // Populate agent select
    jobAgentSelect.innerHTML = '<option value="">Any Agent</option>'; // Default option
    let llmWorkerFound = false;
    const llmWorkerId = "worker-1"; // Assuming this is the ID of your LLM worker

    console.log('Populating agent select in modal with cache:', registered_agents_cache); 
    if (registered_agents_cache && Object.keys(registered_agents_cache).length > 0) {
        Object.values(registered_agents_cache).forEach(agent => {
            if (agent && agent.id && agent.name) {
                const option = document.createElement('option');
                option.value = agent.id;
                option.textContent = `${agent.name} (${agent.id.substring(0,8)})`;
                jobAgentSelect.appendChild(option);
                if (agent.id === llmWorkerId) {
                    llmWorkerFound = true;
                }
            } else {
                console.warn('Skipping agent in dropdown due to missing id or name:', agent);
            }
        });
    } else {
        console.log("No agents in cache to populate dropdown. User can only select 'Any Agent'.");
    }

    // Default to LLM worker if available
    if (llmWorkerFound) {
        jobAgentSelect.value = llmWorkerId;
    }
    
    jobDescriptionInput.value = ''; 
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
            const newJobData = {
                id: result.job_id,
                agent_id: selectedAgentId || llmWorkerId, // Default to LLM if "Any" was chosen but LLM is preferred
                status: "pending", 
                task_type: "user_task", // Or derive from modal if more types are added
                payload: { description: description },
                created_at: new Date().toISOString(),
                result: null, 
                completed_at: null 
            };
            known_jobs_cache[result.job_id] = newJobData; 
            
            // Add to knownInteractions
            knownInteractions[result.job_id] = {
                id: result.job_id,
                type: 'job',
                name: `Job: ${description.substring(0, 20) + "..."}`,
                timestamp: newJobData.created_at,
                originalData: newJobData,
                status: newJobData.status
            };
            renderInteractionList(); // Update the unified list
            await focusInteraction(result.job_id, 'job'); // Focus the new job-interaction

            // Send the job description as the first chat message for this job's interaction history
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'chat',
                    session_id: result.job_id, // Use job_id as session_id for chat history
                    content: `Task: ${description}`, // Prefix to indicate it's the task
                    sender_id: persistentUserId, // Or a system user like "System (Job Task)"
                }));
            }

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
    // This function is called when a 'job_update' WebSocket message is received
    // or when jobs are initially loaded.
    // It updates the job's representation in the knownInteractions list.
    
    known_jobs_cache[job.id] = job; // Update the detailed job cache for quick lookups if needed

    const interactionName = `Job: ${job.payload.description ? job.payload.description.substring(0, 20) + "..." : job.id.substring(0,8)}`;

    if (knownInteractions[job.id]) {
        // Update existing job interaction
        knownInteractions[job.id].originalData = job;
        knownInteractions[job.id].status = job.status;
        knownInteractions[job.id].name = interactionName;
        // If job.updated_at exists and should affect sorting, update timestamp:
        // knownInteractions[job.id].timestamp = job.updated_at || job.created_at;
    } else {
        // Add new job interaction if it wasn't known (e.g., created by another client or loaded initially)
        knownInteractions[job.id] = {
            id: job.id,
            type: 'job',
            name: interactionName,
            timestamp: job.created_at, // Use created_at for initial timestamp
            originalData: job,
            status: job.status
        };
    }
    
    renderInteractionList(); // Re-render the entire list to reflect changes

    // If the updated job is the currently focused interaction, refresh its view
    // (e.g., title, original task if it changed, chat history if new messages arrived due to job update).
    // A simple re-focus can achieve this, though it might clear and reload chat history.
    if (currentChatTarget.type === 'job' && currentChatTarget.id === job.id) {
        // To avoid clearing and fully reloading chat history unnecessarily if only the job status changed in the list,
        // we might want a more targeted update here.
        // For now, a full re-focus is simple. If this causes UX issues (e.g., chat scroll position lost),
        // this part can be refined.
        // Let's update the title directly if it's the current target, and rely on chat messages for other updates.
        document.getElementById('chat-title').textContent = `Job: ${knownInteractions[job.id].name}`;
        // If the original task description itself could change via a job_update, that would also need handling here.
        // For now, assuming only status and result change.
    }
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

document.getElementById('new-chat-button').addEventListener('click', createNewChatSession);
// The 'new-job-button' already has an onclick="openSubmitJobModal()" in the HTML.

// Initialize
initWebSocket();
