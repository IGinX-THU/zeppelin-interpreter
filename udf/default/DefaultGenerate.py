from api.BaseGenerate import UDFBaseGenerate
from default.DefaultUtilities import LLMDao


class UDFDefaultGenerate(UDFBaseGenerate):
    def __init__(self):
        pass

    def generate_prompt(self, base_udf: str, description: str, module_name: str) -> str:
        return f"""
I am a system developer and I would like you to generate a UDF for the user. 
The base class code for the UDF is as follows:
```python
{base_udf}
```
You should import the base class from "{module_name}" module.
Only output the UDF subclass code, without including the base class code. 
Please output the code in plain text format, not in markdown format.
Based on the following description provided by the user, 
generate the content for the UDF subclass file:

{description}
            """.strip()

    def generate_udf(self, prompts: list[str]) -> list[str]:
        return LLMDao().request(prompts)


# Example usage, to run this file directly in udf directory:
#   python -m default.DefaultGenerate
if __name__ == "__main__":
    udfg = UDFDefaultGenerate()
    result = udfg.transform([
        ['type', 'description'],
        ['BINARY', 'BINARY'],
        ["Relate".encode(), 'Please randomly associate 2 pairs of paths'.encode()]
    ], [], {})
    print(result)
