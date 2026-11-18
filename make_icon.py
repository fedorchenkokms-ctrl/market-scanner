# Рисует значок программы icon.ico (вызывается при сборке на GitHub)
from PIL import Image, ImageDraw

img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.ellipse((8, 8, 248, 248), fill=(26, 115, 232, 255))
d.line((56, 176, 104, 120, 144, 152, 200, 72), fill="white", width=22, joint="curve")
img.save("icon.ico", sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])
print("icon ok")
