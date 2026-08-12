// Module 5.8 (Podcast Player): resume-from-position via localStorage (no user
// accounts in MVP, so this is client-side, not server-side state), plus
// deep-link seek support for a search result's relevant-segment timestamp.
(function () {
  const audio = document.getElementById("player");
  if (!audio) return;

  const episodeId = window.location.pathname.split("/").filter(Boolean).pop();
  const storageKey = `podintel:progress:${episodeId}`;

  const seekTo = window.PODINTEL_SEEK_TO;
  audio.addEventListener(
    "loadedmetadata",
    () => {
      if (seekTo != null) {
        audio.currentTime = seekTo;
      } else {
        const saved = localStorage.getItem(storageKey);
        if (saved) audio.currentTime = parseFloat(saved);
      }
    },
    { once: true }
  );

  audio.addEventListener("timeupdate", () => {
    localStorage.setItem(storageKey, String(audio.currentTime));
  });

  document.querySelectorAll(".transcript .segment").forEach((el) => {
    el.style.cursor = "pointer";
    el.addEventListener("click", () => {
      audio.currentTime = parseFloat(el.dataset.start);
      audio.play();
    });
  });
})();
