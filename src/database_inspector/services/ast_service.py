import ast
import inflect
from datetime import datetime

from database_inspector.infrastructure.models import DbColumn


def get_singular(word: str) -> str:
    engine = inflect.engine()
    maybe_singular = engine.singular_noun(word)
    if maybe_singular is not False and isinstance(maybe_singular, str):
        return maybe_singular
    return word


def create_dataclass_ast(class_name: str, table_rows: list[DbColumn]) -> ast.Module:
    import_datetime = False

    dataclass_decorator = ast.Name(id="dataclass", ctx=ast.Load())

    class_def = ast.ClassDef(
        name=get_singular(class_name).title(),
        bases=[],
        keywords=[],
        body=[],
        decorator_list=[
            dataclass_decorator,
        ],
    )

    for row in table_rows:
        if row.datatype == datetime:
            import_datetime = True

        row_datatype = row.datatype.__name__ if row.datatype else "None"

        attr = ast.AnnAssign(
            target=ast.Name(id=row.name, ctx=ast.Store()),
            annotation=ast.Name(id=row_datatype, ctx=ast.Load()),
            value=None,
            simple=1,
        )
        class_def.body.append(attr)

    import_statements = [
        ast.ImportFrom(
            module="dataclasses",
            names=[ast.alias(name="dataclass", asname=None)],
            level=0,
        ),
    ]

    if import_datetime:
        import_statements.append(
            ast.ImportFrom(
                module="datetime",
                names=[ast.alias(name="datetime", asname=None)],
                level=0,
            )
        )

    module = ast.Module(body=[*import_statements, class_def], type_ignores=[])

    return module
