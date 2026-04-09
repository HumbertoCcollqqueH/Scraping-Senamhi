# Scraping-Senamhi

Proyecto de scraping web para la descarga de datos hidrometeorológicos históricos desde el portal de **SENAMHI**, desarrollado con **Python**, **Flask**, **Selenium** y una interfaz web local.

Este sistema permite:

- seleccionar una región del Perú,
- inventariar las estaciones disponibles en el mapa,
- elegir manualmente una estación,
- abrirla en el navegador,
- validar la vista de datos,
- resolver CAPTCHA o Turnstile si aparece,
- y descargar el histórico disponible en archivos **CSV**.

---

## Descripción general

El proyecto implementa un flujo **semiautomático** de scraping.

Primero, el sistema analiza una región y detecta las estaciones disponibles.  
Después, el usuario elige una estación desde la interfaz web.  
Luego, el scraper abre esa estación en Chrome para que el usuario:

1. cambie a la pestaña **Tabla**,
2. resuelva el CAPTCHA si aparece,
3. confirme que los datos están visibles.

Finalmente, el sistema descarga los periodos disponibles de esa estación en formato CSV.

> Importante: este proyecto **no busca evadir CAPTCHA**.  
> Cuando aparece una validación, el usuario debe resolverla manualmente.

---

## Características

- Interfaz web local desarrollada con HTML, CSS y JavaScript.
- Backend en Flask con rutas API para controlar el proceso.
- Inventario automático de estaciones por región.
- Selección manual de estación desde la interfaz.
- Apertura de estación en navegador Chrome.
- Descarga histórica por periodos en archivos CSV.
- Visualización de progreso, logs, errores y archivos descargados.
- Soporte para modo **headless** o con navegador visible.
- Organización automática de archivos por región, tipo de estación y nombre de estación.

---

## Tecnologías utilizadas

- Python 3
- Flask
- Flask-CORS
- Selenium
- webdriver-manager
- HTML / CSS / JavaScript

---

## Requisitos

Antes de ejecutar el proyecto, asegúrate de tener instalado:

- Python 3.10 o superior
- Google Chrome
- pip

---

## Instalación

Clona el repositorio:

```bash
git clone https://github.com/HumbertoCcollqqueH/Scraping-Senamhi.git
cd Scraping-Senamhi
