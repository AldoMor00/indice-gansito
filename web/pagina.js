// Lightbox: cualquier <a class="lupa"> abre la imagen de su href en un <dialog> sobre la página.
// Se cierra con Esc, con el botón, o haciendo clic fuera de la imagen.
const dialogo = document.createElement("dialog");
dialogo.className = "lupa-dialogo";
dialogo.innerHTML = '<img alt=""><button type="button">cerrar · esc</button>';
document.body.append(dialogo);
const imagen = dialogo.querySelector("img");

document.addEventListener("click", (evento) => {
  const enlace = evento.target.closest("a.lupa");
  if (enlace) {
    evento.preventDefault();
    // getAttribute y no .href: en los <a> de SVG (los nodos del diagrama) .href es un objeto.
    imagen.src = enlace.getAttribute("href");
    imagen.alt = enlace.dataset.alt ?? enlace.querySelector("img")?.alt ?? "";
    dialogo.showModal();
    return;
  }
  if (evento.target === dialogo || evento.target.closest(".lupa-dialogo button")) {
    dialogo.close();
  }
});

dialogo.addEventListener("close", () => {
  imagen.removeAttribute("src");
});
