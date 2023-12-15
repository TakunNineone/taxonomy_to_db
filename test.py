import xml.dom.minidom as xml

doc = xml.Document()

declaration = doc.toxml()

a = doc.createElement("A")
doc.appendChild(a)
b = doc.createElement("B")
a.appendChild(b)

xml = doc.toprettyxml()[len(declaration):]

file_handle = open("temp.xml", "w", encoding='utf-8')
doc.writexml(file_handle)
file_handle.close()