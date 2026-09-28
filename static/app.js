function copyToken(button) {
  const box = button.closest('.token-box');
  const textarea = box ? box.querySelector('textarea') : null;
  if (!textarea) return;

  const text = textarea.value;
  const done = () => {
    const oldText = button.textContent;
    button.textContent = 'Copied';
    button.disabled = true;
    setTimeout(() => {
      button.textContent = oldText || 'Copy';
      button.disabled = false;
    }, 1200);
  };

  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).then(done).catch(() => {
      textarea.focus();
      textarea.select();
      document.execCommand('copy');
      done();
    });
  } else {
    textarea.focus();
    textarea.select();
    document.execCommand('copy');
    done();
  }
}

function playNotificationSound() {
  try {
    const AudioContextClass = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextClass) return;
    const context = new AudioContextClass();
    const oscillator = context.createOscillator();
    const gain = context.createGain();
    oscillator.type = 'sine';
    oscillator.frequency.setValueAtTime(880, context.currentTime);
    oscillator.frequency.setValueAtTime(1320, context.currentTime + 0.12);
    gain.gain.setValueAtTime(0.001, context.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.16, context.currentTime + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, context.currentTime + 0.35);
    oscillator.connect(gain).connect(context.destination);
    oscillator.start();
    oscillator.stop(context.currentTime + 0.36);
  } catch (_) {
    // Browsers may block sound until the user has interacted with the page.
  }
}

window.playNotificationSound = playNotificationSound;

function showNewTaskNotice() {
  const existing = document.getElementById('new-task-notice');
  if (existing) existing.remove();
  const notice = document.createElement('button');
  notice.id = 'new-task-notice';
  notice.type = 'button';
  notice.textContent = '有新订单提交，正在更新任务列表…';
  notice.style.cssText = 'position:fixed;right:24px;bottom:24px;z-index:9999;padding:14px 18px;border:0;border-radius:10px;background:#c62828;color:#fff;font-weight:700;box-shadow:0 4px 16px rgba(0,0,0,.25);cursor:pointer;';
  notice.addEventListener('click', () => window.location.reload());
  document.body.appendChild(notice);
}

if (window.enableTaskPolling) {
  let latestTaskId = null;
  const pollNewTasks = () => {
    fetch('/ops/tasks/poll', { credentials: 'same-origin' })
      .then(response => response.ok ? response.json() : null)
      .then(data => {
        if (!data) return;
        if (latestTaskId !== null && data.latest_task_id > latestTaskId) {
          playNotificationSound();
          showNewTaskNotice();
          window.setTimeout(() => window.location.reload(), 900);
        }
        latestTaskId = data.latest_task_id;
      })
      .catch(() => {});
  };
  pollNewTasks();
  window.setInterval(pollNewTasks, 10000);
}

if (window.enableTaskMessagePolling) {
  let latestMessageId = null;
  const pollTaskMessages = () => {
    fetch('/ops/messages/poll', { credentials: 'same-origin' })
      .then(response => response.ok ? response.json() : null)
      .then(data => {
        if (!data) return;
        if (latestMessageId !== null && data.latest_message_id > latestMessageId) {
          playNotificationSound();
          window.setTimeout(() => window.location.reload(), 450);
        }
        latestMessageId = data.latest_message_id;
      })
      .catch(() => {});
  };
  pollTaskMessages();
  window.setInterval(pollTaskMessages, 15000);
}
