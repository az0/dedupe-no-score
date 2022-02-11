def go():
    print('This will bypass the serialization/deserialization process in case the issue is there')
    import train_from_csv
    import record_linkage
    linker = train_from_csv.go()
    record_linkage.go(linker)

if __name__ == "__main__":
    go()
    