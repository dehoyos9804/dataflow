import argparse
import unicodedata
import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def sanitizar_palabra(palabra: str)-> str:
    """
    sanitiza una palabra de textos
    :param palabra:
    :return:
    """
    para_quitar = [',', '.', '-', ':' ' ', "'", '"']

    for simbolo in para_quitar:
        palabra = palabra.replace(simbolo, '')

    return str(unicodedata.normalize('NFKD', palabra).encode('ASCII', 'ignore').strip().lower().decode())


def main():
    # lectura de argumentos de entrada de cli
    parser = argparse.ArgumentParser(description="Primer pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")
    parser.add_argument("--n-palabras", type=int, help="Número de palabras en la salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida
    n_palabras = custom_args.n_palabras

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p: # manera recomendada de ejecutar pipelie es utilizando with
        lineas: PCollection[str] = p | "Leer Entrada" >> beam.io.ReadFromText(entrada)

        # ejemplo de lectura de la linea del texto y lo convertimos en una lista
        # "En un lugar de la Mancha" --> ["En", "un", "lugar", "de", "la", "Mancha"], [...], [...]
        palabras = lineas | "Pasamos a palabras" >> beam.FlatMap(lambda l: l.split())
        limpiadas = palabras | "Sanitizar palabras" >> beam.Map((sanitizar_palabra))
        # contar palabras
        contadas: PCollection[tuple[str]] = limpiadas | "Contamos Palabras" >> beam.combiners.Count.PerElement()
        palabras_top_lista = contadas | "Calculamos Ranking de palabras" >> beam.combiners.Top.Of(n_palabras, key=lambda kv: kv[1])
        palabras_top = palabras_top_lista | "Desenvuelve lista" >> beam.FlatMap(lambda x: x)
        formateado: PCollection[str] = palabras_top | "Formateamos datos" >> beam.Map(lambda kv: "%s, %d" % (kv[0], kv[1]))
        # formateado | beam.Map(print)
        formateado | "Escribimos Salida" >> beam.io.WriteToText(salida)


if __name__ == '__main__':
    main()