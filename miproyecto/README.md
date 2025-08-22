Pasos si usas GitHub Codespaces (de 0 a UI)

1. **Crea un repo** en GitHub (por ejemplo `ejemplo`).
2. **Añade `requirements.txt`** en la raíz con:

   ```
   pandas
   create-dagster
   dagster
   dagster-webserver
   dagster-dg-cli
   ```

   (Estos paquetes instalan el SDK, el webserver/UI y las CLIs `create-dagster` y `dg`.) ([PyPI][6])
3. **Abre un Codespace** sobre el repo.
4. En la terminal, **crea y activa un venv** y **instala dependencias**:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
5. **Genera el proyecto Dagster**:

   ```bash
   create-dagster project miproyecto
   cd miproyecto
   ```

   (La CLI `create-dagster` genera la estructura recomendada de proyecto.) ([docs.dagster.io][5])
6. **Crea un archivo de assets** con el scaffold de `dg`:

   ```bash
   dg scaffold defs dagster.asset assets.py
   ```

   (Crea el esqueleto donde pegarás tus assets.) ([docs.dagster.io][7])
7. **Arranca la UI**:

   ```bash
   dg dev
   ```

   El servidor web se inicia y la UI queda disponible, **usualmente en el puerto 3000**. En Codespaces, ábrelo desde **Ports** → **3000** → **Open in Browser**. ([docs.dagster.io][8], [GitHub][9])
8. **Explora la UI**: pestañas de **Assets**, **Automation/Schedules**, el **DAG/Graph**, y la **página de runs/logs**. ([docs.dagster.io][8])

> Nota: Dagster recomienda `uv`, pero con `pip` en Codespaces funciona perfecto; las CLIs `create-dagster` y `dg` cubren la creación y el desarrollo local. ([docs.dagster.io][10])

---

# 3) Construye el flujo de ejemplo (assets + checks + automation)

**Objetivo:** `clientes → nombres_preparados → nombres_por_persona → cuentas_todas_positivas (check) → generar_reporte`

### 3.1. Prepara un CSV de ejemplo

Descarga **customers-100.csv** y colócalo en tu repo (p.ej. `/workspaces/ejemplo/customers-100.csv` dentro del Codespace):

```bash
curl -L -o customers-100.csv \
  https://drive.google.com/uc?id=1x2IdSNcHGLmot9i1h90gwMJr5lULC2QV&export=download
```

(Conjuntos de ejemplo mantenidos por Datablist.) ([datablist.com][11], [GitHub][12])

### 3.2. Usa el siguiente código en `assets.py`

Tu código ya incluye:

* **Schedule en `clientes`** con `AutomationCondition.on_cron("0 0 * * 1")` (cada lunes a las 00:00).
* **Reactividad con `eager()`** en el resto para que materialicen cuando cambie su upstream.
* **Metadata** con `context.add_output_metadata(...)` para previsualizar resultados.
* **Asset check** **blocking** para asegurar que no hay conteos negativos.
  Todo esto está alineado con las guías de **automation**, **metadata** y **asset checks** del tutorial y la doc oficial. ([docs.dagster.io][2], [erin-essentials-v1-partitions.dagster.dagster-docs.io][13])

```python
import dagster as dg
import pandas as pd


@dg.asset(automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"))
def clientes() -> pd.DataFrame:
    return pd.read_csv("/workspaces/ejemplo-dagster/customers-100.csv")


@dg.asset(automation_condition=dg.AutomationCondition.eager())
def nombres_preparados(context: dg.AssetExecutionContext, clientes: pd.DataFrame) -> pd.DataFrame:
    resultado = (
        clientes[["Customer Id", "First Name"]]
        .drop_duplicates()
        .rename(columns={"Customer Id": "id", "First Name": "nombre"})
    )
    context.add_output_metadata({'previa': dg.MetadataValue.text(str(resultado.head()))})
    return resultado


@dg.asset(automation_condition=dg.AutomationCondition.eager())
def nombres_por_persona(nombres_preparados: pd.DataFrame) -> pd.DataFrame:
    return (
        nombres_preparados.groupby("nombre")
        .count()
        .reset_index()
        .rename(columns={"id": "total"})
    )


@dg.asset_check(
    asset=nombres_por_persona,
    description="Valida valores positivos",
    blocking=True,
)
def cuentas_todas_positivas(nombres_por_persona: pd.DataFrame) -> dg.AssetCheckResult:
    negativos = nombres_por_persona.loc[lambda x: x["total"] < 0]
    return dg.AssetCheckResult(passed=negativos.empty)


@dg.asset(automation_condition=dg.AutomationCondition.eager())
def generar_reporte(nombres_por_persona: pd.DataFrame) -> int:
    nombres_por_persona.to_csv("/workspaces/ejemplo-dagster/mireporte.csv", index=False)
    return len(nombres_por_persona)
```

> Esto crea una automatizacion, pero revisa como crear un Job o Schedule para agrupar a estos assets de manera mas efectiva.

---

# 4) Observar en la UI

* **Materializar `clientes`** (o esperar al cron), ver **lineage** y dependencias. ([docs.dagster.io][8])
* **Ver metadata** agrega metadatos usando `context` en todos los assets `nombres_preparados`. 
* **Ejecutar/inspeccionar el check** `cuentas_todas_positivas` y comprobar que **bloquea** si falla antes de `generar_reporte`. ([docs.dagster.io][3])
* **Abrir el archivo** `mireporte.csv` generado en la ruta del workspace.



Fuentes

[1]: https://docs.dagster.io/etl-pipeline-tutorial?utm_source=chatgpt.com "Build an ETL Pipeline | Dagster Docs"
[2]: https://docs.dagster.io/etl-pipeline-tutorial/automate-your-pipeline?utm_source=chatgpt.com "Automate your pipeline | Dagster Docs"
[3]: https://docs.dagster.io/etl-pipeline-tutorial/data-quality?utm_source=chatgpt.com "Ensure data quality with asset checks | Dagster Docs"
[4]: https://docs.dagster.io/etl-pipeline-tutorial/visualize-data?utm_source=chatgpt.com "Build a dashboard to visualize data | Dagster Docs"
[5]: https://docs.dagster.io/guides/build/projects/creating-a-new-project?utm_source=chatgpt.com "Creating a new Dagster project"
[6]: https://pypi.org/project/dagster/?utm_source=chatgpt.com "dagster · PyPI"
[7]: https://docs.dagster.io/guides/build/assets/defining-assets?utm_source=chatgpt.com "Defining assets - Dagster Docs"
[8]: https://docs.dagster.io/guides/operate/webserver?utm_source=chatgpt.com "Dagster webserver and UI | Dagster Docs"
[9]: https://github.com/dagster-io/dagster-quickstart/blob/main/README.md?utm_source=chatgpt.com "dagster-quickstart/README.md at main - GitHub"
[10]: https://docs.dagster.io/getting-started/installation?utm_source=chatgpt.com "Installing Dagster | Dagster Docs"
[11]: https://www.datablist.com/learn/csv/download-sample-csv-files?utm_source=chatgpt.com "Download Sample CSV Files for free - Datablist"
[12]: https://github.com/datablist/sample-csv-files?utm_source=chatgpt.com "GitHub - datablist/sample-csv-files"
[13]: https://erin-essentials-v1-partitions.dagster.dagster-docs.io/tutorial/building-an-asset-graph?utm_source=chatgpt.com "Tutorial, part four: Building an asset graph | Dagster Docs"
[14]: https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/customizing-on-cron-condition?utm_source=chatgpt.com "Customizing on_cron | Dagster Docs"
[15]: https://newreleases.io/project/github/dagster-io/dagster/release/1.8.12?utm_source=chatgpt.com "dagster-io/dagster 1.8.12 on GitHub - NewReleases.io"
