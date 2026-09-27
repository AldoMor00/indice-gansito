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

// El encadenado (sección 03): con mouse se resalta el eslabón bajo el cursor; con el dedo, el
// último que se tocó, hasta tocar fuera. El eslabón lo dicen las zonas invisibles del SVG.
const encadenado = document.querySelector(".encadenado");

function activaEslabon(eslabon) {
  if (eslabon) encadenado.dataset.activo = eslabon;
  else delete encadenado.dataset.activo;
}

encadenado.addEventListener("pointerover", (evento) => {
  if (evento.pointerType === "mouse") activaEslabon(evento.target.dataset.eslabon);
});
encadenado.addEventListener("pointerleave", (evento) => {
  if (evento.pointerType === "mouse") activaEslabon();
});
document.addEventListener("pointerdown", (evento) => activaEslabon(evento.target.dataset?.eslabon));

// Y una sola vez, cuando el diagrama sube a 75 % de la ventana, los eslabones se arman uno tras
// otro. Con scroll y no con IntersectionObserver por la misma razón que el índice: si la página
// carga ya más abajo, el observer nunca avisa y los pares se quedarían escondidos. Con
// prefers-reduced-motion las dos clases no hacen nada.
encadenado.classList.add("espera");

function reproduceEncadenado() {
  if (encadenado.getBoundingClientRect().top > innerHeight * 0.75) return;
  removeEventListener("scroll", reproduceEncadenado);
  encadenado.classList.replace("espera", "reproduce");
  encadenado.querySelector("tspan.e3").addEventListener("animationend", () => {
    encadenado.classList.remove("reproduce");
  }, { once: true });
}

addEventListener("scroll", marcaSeccion, { passive: true });
addEventListener("scroll", reproduceEncadenado, { passive: true });
addEventListener("resize", avisaDesborde, { passive: true });
barra.addEventListener("scroll", avisaDesborde, { passive: true });
marcaSeccion();
avisaDesborde();
reproduceEncadenado();
