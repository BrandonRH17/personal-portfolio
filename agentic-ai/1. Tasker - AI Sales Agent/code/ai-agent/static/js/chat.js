
document.getElementById("notify").addEventListener("focus", toggleChat);

function toggleChat() {
    const chat = document.getElementById("chat-widget");
    chat.classList.toggle("chat-visible");
}

function sendMessage() {
    const input = document.getElementById('userInput');
    const message = input.value.trim();
    if (!message) return;

    const chat = document.getElementById('chat-messages');
    const userMsg = document.createElement('div');
    userMsg.textContent = message;
    chat.appendChild(userMsg);

    input.value = '';
    document.getElementById('typingIndicator').style.display = 'block';

    fetch('/send_message', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ user_input: message })
    })
    .then(res => res.json())
    .then(data => {
        const assistantMsg = document.createElement('div');
        assistantMsg.textContent = data.assistant_response;
        chat.appendChild(assistantMsg);
        document.getElementById('typingIndicator').style.display = 'none';
    });
}
