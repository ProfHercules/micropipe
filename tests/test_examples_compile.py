import glob


def test_examples_compile():
    root_dir = "./examples"
    examples = glob.glob(root_dir + f"**/*.py", recursive=True)

    for example in examples:
        with open(example, "r") as ex:
            x = compile(source=ex.read(), filename=example, mode="exec")
            print(x)
