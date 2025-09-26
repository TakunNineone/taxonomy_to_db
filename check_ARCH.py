import zipfile
import os

def find_non_xml_xsd_files_in_archive(archive_path):
    """
    Проверяет ZIP-архив и выводит все пути файлов,
    расширение которых отличается от .xml и .xsd.

    :param archive_path: путь к ZIP-архиву
    """
    try:
        with zipfile.ZipFile(archive_path, 'r') as archive:
            # Получаем список всех файлов и папок в архиве
            all_files = archive.namelist()

            # Фильтруем файлы: оставляем только те, что не заканчиваются на .xml или .xsd
            non_xml_xsd_files = [
                file_path for file_path in all_files
                if not file_path.endswith('/')  # исключаем папки
                and not (file_path.lower().endswith('.xml') or file_path.lower().endswith('.xsd'))
            ]

            # Выводим результат
            if non_xml_xsd_files:
                print("Файлы с расширениями, отличными от .xml и .xsd:")
                for file_path in non_xml_xsd_files:
                    print(file_path)
            else:
                print("В архиве нет файлов с расширениями, отличными от .xml и .xsd.")

    except FileNotFoundError:
        print(f"Ошибка: Архив '{archive_path}' не найден.")
    except zipfile.BadZipFile:
        print(f"Ошибка: '{archive_path}' не является корректным ZIP-архивом.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

find_non_xml_xsd_files_in_archive("final_8_0.zip")