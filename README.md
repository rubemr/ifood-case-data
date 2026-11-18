# Case Data Architect — iFood

Ingestão e análise de corridas Yellow Taxi de Nova York, de janeiro a maio de 2023, com PySpark, Databricks e Delta Lake.

## Arquitetura e decisões

`TLC → landing (Parquet original) → Bronze (Delta) → Silver (Delta) → Gold (view SQL e agregados)`

- **Landing:** preserva os arquivos originais, versões por SHA-256 e manifesto de validação. O ponteiro ativo só é publicado após conferir os bytes remotos.
- **Bronze:** usa PySpark, mantém as colunas de origem e acrescenta proveniência. Reexecuções substituem a fatia do arquivo mensal com `replaceWhere`, evitando duplicação da carga.
- **Silver:** preserva todas as corridas, mantém valores raw e aplica tipos e flags de qualidade. Testa 15 cenários sintéticos e reconcilia a transformação com a Bronze.
- **Gold:** expõe `workspace.ifood_gold.yellow_trips` para SQL e seis tabelas agregadas com as respostas e análises de sensibilidade.

Batch atende aos cinco arquivos históricos, sem a complexidade de streaming. Unity Catalog fornece os metadados; managed tables e volumes permitem demonstrar a solução na Free Edition sem bucket externo. Escritas Delta têm transações por tabela; o pipeline inteiro não é uma única transação.

As cinco colunas obrigatórias estão na view de consumo: `VendorID INT`, `passenger_count INT`, `total_amount DECIMAL(18,2)`, `tpep_pickup_datetime TIMESTAMP_NTZ` e `tpep_dropoff_datetime TIMESTAMP_NTZ`. Horários preservam o relógio local de Nova York. `source_file_month` identifica a origem; o pickup define o mês analítico. `VendorID` identifica o provedor, não o veículo.

## Resultados

Foram preservadas **16.186.386 corridas** dos cinco arquivos.

| Pickup | Total reportado (USD) | Ticket médio (USD) |
|---|---:|---:|
| Janeiro | 82.863.594,11 | 27,02 |
| Fevereiro | 78.381.600,43 | 26,90 |
| Março | 94.634.027,66 | 27,80 |
| Abril | 92.956.843,11 | 28,27 |
| Maio | 101.765.282,92 | 28,96 |

A primeira pergunta admite duas leituras: **média dos cinco totais mensais: USD 90.120.269,65**; **valor médio por corrida de cada mês:** coluna ticket acima. O total do período é USD 450.601.348,23 e o ticket global ponderado é USD 27,84.

Em maio, a média de passageiros reportados é maior às **02h (1,4366)** e menor às **06h (1,2347)**. A resposta das 24 horas está em [passageiros_por_hora_maio.csv](analysis/passageiros_por_hora_maio.csv). A média ponderada do mês é **1,3588**; considerando apenas positivos, **1,3830**.

[Resultados e gráficos](analysis/resultados.md) · [Exploração de janeiro](analysis/eda_janeiro.md).

## Qualidade

No arquivo de janeiro há 71.743 passageiros NULL, 51.164 zeros, 25.204 valores totais negativos e 48 pickups fora do mês nominal. Nenhuma dessas corridas é apagada da Bronze ou da Silver.

- Passageiros NULL permanecem desconhecidos: não recebem zero e não entram na média dessa medida.
- Zeros reportados permanecem na média principal; a comparação apenas com positivos explicita o impacto dessa decisão. O significado do zero não foi confirmado.
- Passageiros negativos, fracionários ou não representáveis são sinalizados e não entram na média; o valor de origem é preservado.
- Valores monetários negativos e zero permanecem na análise principal; a comparação sem negativos é publicada separadamente.
- Datas fora da janela são sinalizadas e excluídas das métricas do período. Não há deduplicação por cinco campos, pois eles não formam uma chave única de corrida.

A média descreve os valores informados entre corridas elegíveis; não comprova ocupação real sem viés de toda a frota. Nulos e zeros se associam a atributos observados, sem comprovar um mecanismo de ausência. Preservar uma medida desconhecida como NULL é compatível com a [orientação de Kimball](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/fact-table-null/). A origem e o dicionário estão na [TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Execução

Requisitos: Python 3.12, Git, Databricks CLI 1.22.1 ou compatível e um workspace Free Edition com catálogo `workspace` gravável. Spark é fornecido pelo compute serverless do Databricks. Execute os comandos na raiz do clone; substitua `SEU_WORKSPACE` pelo host da sua conta.

```powershell
git clone https://github.com/rubemr/ifood-case-data.git
Set-Location ifood-case-data
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
databricks auth login --host https://SEU_WORKSPACE --profile ifood-case
.\.venv\Scripts\python.exe src/sync_workspace.py
```

No Databricks, abra `/Shared/ifood-case-data/notebooks/00_setup` e execute com os widgets padrão. Isso cria schemas e o volume de landing, sem sobrescrever tabelas existentes. Depois, no terminal:

```powershell
.\.venv\Scripts\python.exe src/download_sources.py --months 2023-01 2023-02 2023-03 2023-04 2023-05
if ($LASTEXITCODE -ne 0) { throw 'Falha no download' }
.\.venv\Scripts\python.exe src/compare_source_schemas.py
if ($LASTEXITCODE -ne 0) { throw 'Schema incompatível' }
foreach ($month in @('2023-01','2023-02','2023-03','2023-04','2023-05')) {
    .\.venv\Scripts\python.exe src/publish_landing.py --month $month
    if ($LASTEXITCODE -ne 0) { throw "Falha na publicação: $month" }
}
databricks jobs submit --json '@src/run_case.json' --no-wait --profile ifood-case --output json
```

O Job executa nove tarefas sequenciais: Bronze dos cinco meses, exploração de janeiro, Silver e as duas análises. Aguarde `SUCCESS` em todas as tarefas. Consulte a execução com `databricks jobs get-run ID_DA_EXECUCAO --profile ifood-case --output json`.

Também é possível executar os notebooks: `01_bronze` uma vez para cada mês, em ordem de janeiro a maio; `02_profile_bronze` para janeiro; `03_silver`; `04_financial_analysis`; `05_passenger_analysis`. Mantenha `catalog=workspace`, `schema_prefix=ifood`, `landing_catalog=workspace` e `landing_schema=ifood_landing` em todas as etapas. Os SQLs necessários às respostas estão incorporados nos dois notebooks de análise.

Para atualizar os seis CSVs e gerar os gráficos e o relatório local:

```powershell
.\.venv\Scripts\python.exe src/export_results.py --run-id ID_DA_EXECUCAO
if ($LASTEXITCODE -ne 0) { throw 'Exportação inválida' }
.\.venv\Scripts\python.exe src/render_results.py
```

O exportador exige sucesso, mesmo snapshot Silver e igualdade entre CSVs e resultados das tarefas. Revise os três PNGs após regenerar os gráficos. As tabelas agregadas ficam em `workspace.ifood_gold`; os CSVs remotos ficam em `/Volumes/workspace/ifood_gold/results`.

## Estrutura

```text
src/          # Notebooks, ingestão, execução e geração dos resultados
analysis/     # Exploração, respostas, seis CSVs e três gráficos
tests/        # Validação de fontes e configuração de destinos
README.md
requirements.txt
requirements.lock.txt
```

Os testes locais cobrem arquivos inválidos, contrato de colunas, versões imutáveis e configuração. Os notebooks reconciliam contagens, numeradores e denominadores; a análise horária testa nove registros sintéticos. Os cinco arquivos delimitam a cobertura: arquivos adjacentes não foram incluídos. Total reportado não é lucro ou recebimento líquido confirmado. Não há inferência causal, extrapolação anual ou média por veículo.
