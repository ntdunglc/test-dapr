let ws = null;
const clientId = Math.random().toString(36).substring(7);

// Initialize WebSocket connection
function initWebSocket() {
    ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`);
    
    ws.onopen = () => {
        console.log('Connected to coordinator');
        loadInitialData();
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
            displayChatMessage(message.data);
            break;
        case 'agent_update':
            updateAgentDisplay(message.data);
            break;
    }
}

// Load initial data
async function loadInitialData() {
    // Load agents
    const agentsResponse = await fetch('/api/agents');
    const agents = await agentsResponse.json();
    agents.forEach(agent => updateAgentDisplay(agent));
    
    // Load recent jobs
    const jobsResponse = await fetch('/api/jobs/recent');
    const jobs = await jobsResponse.json();
    jobs.forEach(job => updateJobDisplay(job));
    
    // Load chat history
    const chatResponse = await fetch('/api/chat/history/global');
    const chatHistory = await chatResponse.json();
    chatHistory.messages.forEach(msg => displayChatMessage(msg));
}

// Submit a new job
async function submitJob() {
    const taskTypes = ['data_processing', 'analysis'];
    const taskType = taskTypes[Math.floor(Math.random() * taskTypes.length)];
    
    const payload = {
        item_count: Math.floor(Math.random() * 1000) + 100,
        priority: Math.random() > 0.5 ? 'high' : 'normal'
    };
    
    const response = await fetch('/api/jobs/submit', {
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
    
    if (message && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'chat',
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

// Initialize
initWebSocket();
