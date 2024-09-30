"""Console script for fin_chatbot."""
import fin_chatbot

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for fin_chatbot."""
    console.print("Replace this message by putting your code into "
               "fin_chatbot.cli.main")
    console.print("See Typer documentation at https://typer.tiangolo.com/")
    


if __name__ == "__main__":
    app()
