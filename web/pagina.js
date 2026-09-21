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

// Índice de secciones: marca la que está en pantalla. Se calcula con la posición de cada
// sección contra la cabecera y no con IntersectionObserver, porque lo que interesa es
// "cuál empezó ya", no "cuál se ve": las secciones son más altas que la ventana.
const enlacesIndice = [...document.querySelectorAll(".indice a")];
const secciones = enlacesIndice.map((enlace) => document.querySelector(enlace.getAttribute("href")));
let marcada = 0;

function marcaSeccion() {
  const linea = document.querySelector(".cabecera").offsetHeight + 8;
  let actual = 0;
  secciones.forEach((seccion, i) => {
    if (seccion && seccion.getBoundingClientRect().top <= linea) actual = i;
  });
  if (actual === marcada && enlacesIndice[actual].hasAttribute("aria-current")) return;
  enlacesIndice[marcada].removeAttribute("aria-current");
  enlacesIndice[actual].setAttribute("aria-current", "true");
  marcada = actual;
  // Cuando la barra no cabe, la sección marcada puede quedar fuera de vista: se centra.
  // Se mueve scrollLeft a mano y no con scrollIntoView, porque la barra vive en una cabecera
  // fija y scrollIntoView termina arrastrando la página entera de vuelta al inicio.
  const marca = enlacesIndice[actual];
  barra.scrollLeft = marca.offsetLeft - (barra.clientWidth - marca.offsetWidth) / 2;
}

// El degradado del borde derecho sólo aparece si hay algo más a la derecha.
const barra = document.querySelector(".indice");
const marcoBarra = document.querySelector(".indice-marco");

function avisaDesborde() {
  const hay = barra.scrollWidth - barra.clientWidth - barra.scrollLeft > 2;
  marcoBarra.toggleAttribute("data-desborda", hay);
}

addEventListener("scroll", marcaSeccion, { passive: true });
addEventListener("resize", avisaDesborde, { passive: true });
barra.addEventListener("scroll", avisaDesborde, { passive: true });
marcaSeccion();
avisaDesborde();
