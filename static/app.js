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
