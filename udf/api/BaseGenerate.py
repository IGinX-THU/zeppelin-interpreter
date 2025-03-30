import os
from abc import abstractmethod


class UDFBaseGenerate:
    """
    Serves as a foundation for generating user-defined function (UDF) code
    based on an existing base class body and user descriptions. Subclasses
    typically employ language models or templating to produce new source code.
    """

    @abstractmethod
    def generate_prompt(self, base_udf:str, description:str, module_name:str) -> str:
        """
        Must return a prompt that integrates the base class code with the
        user's description. Subclasses often reformat or augment the text input
        for downstream generation processes.
        """
        pass

    @abstractmethod
    def generate_udf(self, prompts: list[str]) -> list[str]:
        """
        Must return subclasses' generated code as text. Implementations typically
        request code completions or build scripts from prompt inputs.
        """
        pass

    def _get_base_udf_content(self, udf_type:str) -> str:
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        base_udf_file_path = os.path.join(curr_dir, 'Base' + udf_type + '.py')

        with open(base_udf_file_path, 'r') as f:
            return f.read()

    def transform(self, data, args, kvargs):
        print("enter transform UDFDefaultGenerator success: ", kvargs)
        print(f"enter {self.__class__.__name__}")
        print(f"data[0]: {data[0]}")
        print(f"data[1]: {data[1]}")
        print(f"len(data): {len(data)}")
        print(f"args: {args}")
        print(f"kvargs: {kvargs}")

        type_index = data[0].index('type')
        description_index = data[0].index('description')

        types = [row[type_index].decode('utf-8') for row in data[2:]]
        descriptions = [row[description_index].decode('utf-8') for row in data[2:]]

        base_udf_contents = [self._get_base_udf_content(udf_type) for udf_type in types]
        base_udf_module_names = ["api.Base"+udf_type for udf_type in types]
        prompts = [self.generate_prompt(base_udf, description,base_udf_module) for base_udf, description, base_udf_module in zip(base_udf_contents, descriptions, base_udf_module_names)]
        generated_udf = self.generate_udf(prompts)

        return [
            ["(prompt)","(udf)"],
            ["BINARY", "BINARY"],
        ] + [
            [prompt.encode(), udf.encode()]
            for prompt, udf in zip(prompts, generated_udf)
        ]
