import sys

def choose_medias():
    medias_base = ['search', 'social', 'digital']
    if len(sys.argv)==1:
        medias_base
        return medias_base
    else:
        medias_input = [s.replace(',','').lower().strip() for s in sys.argv[1:]]
        invalid_media = [m for m in medias_input if not(m in medias_base)]
        if len(invalid_media) > 0:
            sys.exit(f"\n{'='*50}\ninvalid media entered\n{invalid_media}\nexiting script...\n{'='*50}")
        return medias_input



def main():
    print(medias)

if __name__ == '__main__':
    # sys.argv[0] returns program name
    medias = choose_medias()

    main()
