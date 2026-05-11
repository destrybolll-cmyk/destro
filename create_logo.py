from PIL import Image, ImageDraw

SIZE = 640
img = Image.new("RGB", (SIZE, SIZE), "white")
draw = ImageDraw.Draw(img)

cx, cy = SIZE // 2, SIZE // 2
r = 200

# Cookie shadow
draw.ellipse([cx - r + 10, cy - r + 10, cx + r + 10, cy + r + 10], fill="#d4a06a")

# Cookie body
draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill="#f0c78a")

# Inner lighter circle
draw.ellipse([cx - r + 15, cy - r + 15, cx + r - 15, cy + r - 15], fill="#f5d6a8")

# Chocolate chips
chips = [
    (cx - 80, cy - 100),
    (cx + 90, cy - 70),
    (cx - 40, cy + 90),
    (cx + 60, cy + 60),
    (cx - 110, cy + 20),
    (cx + 30, cy - 120),
    (cx + 120, cy + 100),
    (cx - 90, cy - 30),
    (cx - 140, cy - 70),
    (cx + 10, cy + 130),
    (cx + 140, cy - 10),
    (cx - 30, cy - 150),
]
for x, y in chips:
    draw.ellipse([x - 18, y - 18, x + 18, y + 18], fill="#5c3317")
    draw.ellipse([x - 11, y - 11, x + 11, y + 11], fill="#6b3a1f")
    draw.ellipse([x - 4, y - 4, x + 4, y + 4], fill="#4a2810")

# Eyes
eye_color = "#5c3317"
draw.ellipse([cx - 45, cy - 45, cx - 15, cy - 15], fill=eye_color)
draw.ellipse([cx + 15, cy - 45, cx + 45, cy - 15], fill=eye_color)
draw.ellipse([cx - 40, cy - 40, cx - 20, cy - 20], fill="white")
draw.ellipse([cx + 20, cy - 40, cx + 40, cy - 20], fill="white")

# Smile
draw.arc([cx - 50, cy - 10, cx + 50, cy + 60], 0, 180, fill=eye_color, width=5)

img.save("logo_cookie.png")
print("Logo saved as logo_cookie.png")
