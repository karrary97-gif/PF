from PIL import Image, ImageDraw

img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.ellipse((6, 6, 250, 250), fill=(24, 90, 157, 255))
for r in (36, 62, 88, 112):
    box = (128 - r, 128 - r, 128 + r, 128 + r)
    d.arc(box, 200, 340, fill="white", width=10)
    d.arc(box, 20, 160, fill="white", width=10)
img.save("app.ico", sizes=[(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])
print("app.ico written")
