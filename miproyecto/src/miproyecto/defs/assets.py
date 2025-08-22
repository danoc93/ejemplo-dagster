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