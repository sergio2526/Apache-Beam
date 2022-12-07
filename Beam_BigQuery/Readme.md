# Untitled

---

### Table BigQuery:

[<img alt="BigQuery" src="https://res.cloudinary.com/xaiop/image/upload/v1670442434/Untitled_qedshu.png"/>]

- **Input**:

```python

query = "SELECT * FROM dt-data-analytics.test.clientes WHERE Nombre='Pedro'"
query_results = pipeline | "Input query" >> beam.io.ReadFromBigQuery(query = query,
                                                                             use_standard_sql=True)
```

- **Output**:

(Google Cloud Storage)

```python
query_results | "Write to Cloud Storage" >> beam.io.WriteToText(out_data, file_name_suffix=".txt")
```

`{'Nombre': 'Pedro', 'celular': 'Compa es 3203075766'}`

---

### DirectRunner

```python
python main.py --query "SELECT * FROM dt-data-analytics.test.clientes WHERE Nombre='Pedro'" --out_data out/resultado_query_filtro
```

### DataFlowRunner

```python
python main_DataFlow.py --query "SELECT * FROM dt-data-analytics.test.clientes WHERE Nombre='Pedro'" --out_data gs://pruebas-dt-data-analytics/out/salida
```
