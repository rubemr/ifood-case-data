"""Gera três gráficos e o relatório de respostas a partir dos seis CSVs exportados."""
import csv
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[1]
ANALYSIS = ROOT / "analysis"
CHARTS = ANALYSIS / "charts"
NAVY, TEAL, ORANGE, RED = "#17354A", "#007F86", "#BA650B", "#D92B3A"
MUTED, GRID = "#526675", "#DFE6EA"


def br(value, places=2):
    number = Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return f"{number:,.{places}f}".translate(str.maketrans({",": ".", ".": ","}))


def load_csv(name):
    with (ANALYSIS / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def style(ax):
    ax.set_facecolor("white")
    ax.set_axisbelow(True)
    ax.grid(axis="y", color=GRID, linewidth=.8)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, length=0, pad=8)
    ax.yaxis.label.set_color(MUTED)
    ax.xaxis.label.set_color(MUTED)


def save(fig, name):
    for extension in ("png",):
        fig.savefig(CHARTS / f"{name}.{extension}", dpi=170, facecolor="white")
    plt.close(fig)


def main():
    monthly = load_csv("resultados_mensais.csv")
    summary = load_csv("media_totais_mensais.csv")
    sensitivity_money = load_csv("sensibilidade_financeira.csv")
    hourly = load_csv("passageiros_por_hora_maio.csv")
    sensitivity_people = load_csv("sensibilidade_passageiros_maio.csv")
    passenger_summary = load_csv("resumo_passageiros_maio.csv")
    if len(monthly) != 5 or len(sensitivity_money) != 5 or len(summary) != 1 or len(passenger_summary) != 1:
        raise ValueError("Esperados cinco meses e um resumo de cada análise")
    if [int(row["hora"]) for row in hourly] != list(range(24)) or [int(row["hora"]) for row in sensitivity_people] != list(range(24)):
        raise ValueError("Esperadas 24 horas ordenadas em cada análise horária")
    financial = summary[0]
    period = passenger_summary[0]
    total = Decimal(financial["total_periodo_usd"])
    monthly_mean = Decimal(financial["media_totais_mensais_usd"])
    assert sum(Decimal(row["total_reportado_usd"]) for row in monthly) == total
    assert sum(int(row["corridas_elegiveis"]) for row in monthly) == int(financial["corridas_elegiveis"])
    assert (total / Decimal(5)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == monthly_mean
    assert sum(int(row["corridas_elegiveis"]) for row in hourly) == int(period["corridas_elegiveis"])
    assert sum(int(row["passageiros"]) for row in hourly) == int(period["passageiros"])
    negative = sum(Decimal(row["contribuicao_negativa_usd"]) for row in sensitivity_money)
    main_mean = Decimal(period["media_principal_precisa"])
    positive_mean = Decimal(period["media_positivos_precisa"])
    money_effect = -negative / total * 100
    people_effect = (positive_mean / main_mean - 1) * 100
    growth = (Decimal(monthly[-1]["total_reportado_usd"]) / Decimal(monthly[0]["total_reportado_usd"]) - 1) * 100
    peak = max(hourly, key=lambda row: Decimal(row["media_passageiros_precisa"]))
    trough = min(hourly, key=lambda row: Decimal(row["media_passageiros_precisa"]))
    volume_peak = max(hourly, key=lambda row: int(row["corridas_elegiveis"]))
    CHARTS.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.titlesize": 12, "axes.titleweight": "bold",
                         "axes.labelsize": 10, "text.color": NAVY})

    # Os painéis financeiros usam unidades e eixos separados; as escalas de barras e valores começam em zero.
    fig, axes = plt.subplots(1, 2, figsize=(11.8, 6), gridspec_kw={"width_ratios": [1.45, 1]})
    fig.subplots_adjust(left=.08, right=.97, top=.76, bottom=.19, wspace=.30)
    fig.suptitle("Valores reportados — janeiro a maio de 2023", x=.08, y=.96, ha="left", fontsize=18, weight="bold")
    fig.text(.08, .87, f"Média dos cinco totais mensais: USD {br(monthly_mean)}", fontsize=12, color=TEAL)
    names = ["Jan", "Fev", "Mar", "Abr", "Mai"]
    amounts = [float(Decimal(row["total_reportado_usd"]) / 1_000_000) for row in monthly]
    tickets = [float(row["ticket_medio_usd_preciso"]) for row in monthly]
    ax = axes[0]
    style(ax)
    bars = ax.bar(names, amounts, color=[TEAL] * 4 + [RED], width=.60)
    ax.set_ylim(0, 116)
    ax.set_ylabel("Total reportado (USD milhões)")
    ax.set_title("Total por mês de pickup", loc="left", pad=16)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: br(value, 0)))
    for bar, value in zip(bars, amounts):
        ax.text(bar.get_x() + bar.get_width()/2, value + 2.4, br(value), ha="center", weight="bold", fontsize=10)
    ax.axhline(float(monthly_mean / 1_000_000), color=ORANGE, linestyle="--", linewidth=1.4,
               label=f"Média mensal: {br(monthly_mean/1_000_000)} mi")
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    ax = axes[1]
    style(ax)
    ax.plot(names, tickets, color=NAVY, marker="o", linewidth=2.2)
    ax.set_ylim(0, 35)
    ax.set_title("Ticket médio por corrida", loc="left", pad=16)
    ax.set_ylabel("USD por corrida elegível")
    for position, value in enumerate(tickets):
        ax.annotate(br(value), (position, value), xytext=(0, 10), textcoords="offset points", ha="center", weight="bold")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: br(value, 0)))
    fig.text(.08, .075, f"Fonte: CSVs agregados da Gold. Competência pelo pickup local. Zeros e negativos incluídos.", fontsize=8.5, color=MUTED)
    fig.text(.08, .035, "Total_amount mede cobrança reportada; não inclui gorjetas em dinheiro e não mede lucro ou recebimento líquido.", fontsize=8.5, color=MUTED)
    save(fig, "financeiro_mensal")

    # A média horária de passageiros reportados e o volume usam painéis separados, sem eixos duplos.
    hours = [int(row["hora"]) for row in hourly]
    principal = [float(row["media_passageiros_precisa"]) for row in hourly]
    positive = [float(row["media_positivos_precisa"]) for row in sensitivity_people]
    trips = [int(row["corridas_elegiveis"]) for row in hourly]
    fig, axes = plt.subplots(2, 1, figsize=(11.8, 8), sharex=True, gridspec_kw={"height_ratios": [1.3, 1]})
    fig.subplots_adjust(left=.09, right=.97, top=.81, bottom=.16, hspace=.32)
    fig.suptitle("Maio de 2023 — passageiros reportados por hora", x=.09, y=.97, ha="left", fontsize=18, weight="bold")
    ax = axes[0]
    style(ax)
    ax.plot(hours, principal, color=TEAL, marker="o", markersize=4, linewidth=2, label="Principal: inclui zeros")
    ax.plot(hours, positive, color=ORANGE, linestyle="--", linewidth=1.8, label="Comparação: somente positivos")
    ax.axhline(float(main_mean), color=MUTED, linestyle=":", linewidth=1.5, label=f"Média ponderada de maio: {br(main_mean,4)}")
    ax.set_ylim(1.15, 1.53)
    ax.set_ylabel("Passageiros por corrida")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: br(value, 2)))
    high_hour, low_hour = int(peak["hora"]), int(trough["hora"])
    ax.annotate(f"{high_hour:02d}h: {br(peak['media_passageiros_precisa'],4)}", (high_hour, principal[high_hour]),
                xytext=(high_hour+2.4, 1.487), arrowprops={"arrowstyle": "-", "color": TEAL}, color=TEAL, weight="bold")
    ax.annotate(f"{low_hour:02d}h: {br(trough['media_passageiros_precisa'],4)}", (low_hour, principal[low_hour]),
                xytext=(low_hour+1.5, 1.182), arrowprops={"arrowstyle": "-", "color": TEAL}, color=TEAL, weight="bold")
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, ncol=3, loc="upper center", bbox_to_anchor=(.53,.91), frameon=False, fontsize=9)
    ax = axes[1]
    style(ax)
    ax.bar(hours, trips, width=.72, color=[NAVY if hour == int(volume_peak["hora"]) else "#76B6B8" for hour in hours])
    ax.set_title("Corridas elegíveis somadas no mês", loc="left", pad=12)
    ax.set_ylabel("Mil corridas")
    ax.set_ylim(0, 280000)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: br(value/1000,0)))
    ax.set_xticks(hours, [f"{hour:02d}" for hour in hours])
    ax.set_xlabel("Hora local do pickup (America/New_York)", labelpad=12)
    ax.annotate(f"{br(volume_peak['corridas_elegiveis'],0)} corridas", (int(volume_peak["hora"]), int(volume_peak["corridas_elegiveis"])),
                xytext=(0,8), textcoords="offset points", ha="center", fontsize=9, weight="bold")
    fig.text(.09,.075,"Painel superior com escala ampliada (1,15–1,53). NULL/valores inválidos ficam fora das médias; zero é preservado.",fontsize=8.5,color=MUTED)
    fig.text(.09,.035,f"Denominador principal: {br(period['corridas_elegiveis'],0)} corridas. As 24 horas reconciliam com maio.",fontsize=8.5,color=MUTED)
    save(fig, "passageiros_hora_maio")

    fig, axes = plt.subplots(1, 2, figsize=(11.8, 6))
    fig.subplots_adjust(left=.09,right=.97,top=.75,bottom=.27,wspace=.32)
    fig.suptitle("Regras de qualidade alteram a leitura dos resultados",x=.09,y=.96,ha="left",fontsize=18,weight="bold")
    fig.text(.09,.87,"Comparações de sensibilidade; as regras principais estão identificadas em cada painel.",fontsize=11,color=MUTED)
    panels = [([float(main_mean),float(positive_mean)], ["Principal\ncom zeros","Somente\npositivos"],
               "Passageiros por corrida — maio", "Passageiros por corrida", 1.6, 4),
              ([float(total/1_000_000),float((total-negative)/1_000_000)], ["Principal\ncom negativos","Sem valores\nnegativos"],
               "Valor total — janeiro a maio", "USD milhões", 520, 2)]
    for ax,(values,ticks,title,ylabel,maximum,places) in zip(axes,panels):
        style(ax)
        bars=ax.bar(ticks,values,color=[TEAL,"#AFBDC7"],width=.54)
        ax.set_title(title,loc="left",pad=18)
        ax.set_ylabel(ylabel)
        ax.set_ylim(0,maximum)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _, digits=1 if places==4 else 0: br(value,digits)))
        for bar,value in zip(bars,values):
            ax.text(bar.get_x()+bar.get_width()/2,value+maximum*.025,br(value,places),ha="center",weight="bold")
    fig.text(.09,.155,f"Somente positivos: +{br(people_effect)}% na média.\nMesmo numerador; denominador menor.",fontsize=9.5,color=TEAL)
    fig.text(.58,.155,f"Excluir negativos: +{br(money_effect)}% no total.\nContribuição negativa: USD {br(negative)}.",fontsize=9.5,color=TEAL)
    fig.text(.09,.045,"Zeros e negativos permanecem nos dados. A comparação não presume que sejam erros e não altera a camada Silver.",fontsize=8.5,color=MUTED)
    save(fig,"sensibilidade_regras")

    financial_rows = "\n".join(
        f"| {row['mes']} | {br(row['corridas_elegiveis'], 0)} | {br(row['total_reportado_usd'])} | {br(row['ticket_medio_usd'])} |"
        for row in monthly)
    hourly_rows = "\n".join(
        f"| {int(row['hora']):02d}h | {br(row['corridas_elegiveis'], 0)} | {br(row['media_passageiros_precisa'], 4)} | {br(comparison['media_positivos_precisa'], 4)} |"
        for row, comparison in zip(hourly, sensitivity_people))
    report = f"""# Resultados — Yellow Taxi, janeiro a maio de 2023

## Valor total e ticket médio

A média dos cinco totais mensais é **USD {br(monthly_mean)}**. O total do período é **USD {br(total)}** e o ticket global ponderado é **USD {br(financial['ticket_periodo_usd'])}**.

| Mês de pickup | Corridas elegíveis | Total reportado (USD) | Ticket médio (USD) |
|---|---:|---:|---:|
{financial_rows}

![Totais e tickets mensais](charts/financeiro_mensal.png)

A média dos totais divide a soma dos cinco totais por cinco. O ticket divide o valor pela quantidade de corridas elegíveis; o ticket global não é a média simples dos tickets mensais. As duas interpretações da primeira pergunta estão disponíveis nos CSVs.

## Passageiros por hora em maio

| Hora local | Corridas elegíveis | Média com zeros | Média somente positivos |
|---|---:|---:|---:|
{hourly_rows}

![Passageiros reportados e volume por hora](charts/passageiros_hora_maio.png)

A média ponderada do mês é **{br(main_mean, 4)}**: {br(period['passageiros'], 0)} passageiros reportados / {br(period['corridas_elegiveis'], 0)} corridas elegíveis. Não é a média simples das 24 médias. O maior valor ocorre às {int(peak['hora']):02d}h e o menor às {int(trough['hora']):02d}h. O maior volume elegível ocorre às {int(volume_peak['hora']):02d}h, com {br(volume_peak['corridas_elegiveis'], 0)} corridas somadas no mês.

## Qualidade, sensibilidade e interpretação

![Sensibilidade às regras de qualidade](charts/sensibilidade_regras.png)

- Maio tem o maior total e ticket do recorte; seu total é {br(growth)}% superior ao de janeiro. A comparação não comprova causa nem sazonalidade anual.
- A média inclui zeros reportados e ignora passageiros NULL ou inválidos. Somente positivos resulta em {br(positive_mean, 4)}, aumento de {br(people_effect)}%. Nulo não é imputado como zero.
- Valores monetários negativos permanecem na resposta principal; removê-los aumentaria o total em USD {br(-negative)} ({br(money_effect)}%). Não se presume que sejam erros.
- A média descreve os valores conhecidos reportados por corrida, sem provar ocupação real sem viés. VendorID identifica provedor, não táxi. Total_amount é cobrança reportada, não lucro nem recebimento líquido confirmado.
- A competência usa pickup local nos cinco arquivos solicitados. Eventos fora da janela permanecem armazenados e não entram nas métricas do período; arquivos adjacentes não completam as fronteiras.

[Exploração de janeiro](eda_janeiro.md) · [Arquitetura, regras e execução](../README.md).
"""
    (ANALYSIS / "resultados.md").write_text(report, encoding="utf-8")
    print("Gerados três gráficos e analysis/resultados.md; confira os PNGs antes da entrega.")


if __name__ == "__main__":
    main()
