from avro import schema, datafile, io
from datetime import datetime


OUTFILE_NAME = "test.avro"

SCHEMA_DEFINITION = (
    """{
        "namespace": "avro",
        "name": "testAvro",
        "type": "record",
        "fields": [
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "date",
                "type": "int",
                "logicalType": "date"
            }
        ]
    }"""
)

SCHEMA = schema.parse(SCHEMA_DEFINITION)


def write_avro_file():
    """Creates avro file"""
    writer = datafile.DataFileWriter(
        writer=open(OUTFILE_NAME, "wb"),
        datum_writer=io.DatumWriter(),
        writer_schema=SCHEMA,
        codec="deflate"
    )

    writer.append(
        {
            "name": "Job",
            "date": datetime.now().date()
        }
    )

    writer.close()


def read_avro_file():
    """Reads avro file"""
    reader = datafile.DataFileReader(
        reader=open(OUTFILE_NAME, "rb"),
        datum_reader=io.DatumReader()
    )

    for record in reader:
        print("name:", record["name"])
        print("date:", record["date"])

    reader.close()


if __name__ == "__main__":
    # Create an AVRO file
    write_avro_file()

    # Read the above AVRO file
    read_avro_file()
