def outside():
    def inside(name):
        print(f"hello {name}")

    return inside


def outside_1(name):
    def inside():
        print(f"hello {name}")

    return inside


def outside_2(fn):
    def inside(name):
        print(f"hello {name}")
        fn(name)

    return inside


@outside_2
def bye(name):
    print(f"bye {name}")


f = outside_1("Todd")
f()
i = outside()
i("Ben")
bye("Todd")
# print(f("Todd"))
