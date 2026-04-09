# Proyecto Scraping V12

## Instalación

```bash
pip install -r requirements.txt
```

## Ejecución

```bash
python app.py
```

Luego abre:

```text
http://localhost:5000
```

## Flujo V12

1. Selecciona la región.
2. Pulsa **Analizar estaciones**.
3. Espera a que termine el inventario.
4. Elige una estación en la lista.
5. Pulsa **Abrir estación**.
6. En el navegador Chrome del scraper:
   - cambia manualmente a **Tabla**,
   - resuelve CAPTCHA si aparece,
   - verifica que ya ves los datos.
7. Vuelve a la interfaz y pulsa **Confirmar descarga**.
8. El sistema descargará todos los periodos de esa estación.

## Estructura de guardado

Los archivos se guardan así:

```text
descargas_senamhi_v12/
  region/
    Tipo de estación/
      Nombre de estación/
        2019-01.csv
        2019-02.csv
        ...
```

Ejemplo:

```text
arequipa/
  Convencional - Meteorológica/
    LOMAS/
      2020-01.csv
      2020-02.csv
```

## Notas importantes

- Esta versión no intenta evadir CAPTCHA.
- El control manual ocurre solo antes de iniciar la descarga de la estación elegida.
- La lógica de descarga rápida se mantiene.
