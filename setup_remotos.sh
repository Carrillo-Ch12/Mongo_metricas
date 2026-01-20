#!/bin/bash

# 1. Creamos el contenido del script de limpieza localmente
cat > /tmp/clean_ram.sh << 'EOF'
#!/bin/bash
sync
echo 3 > /proc/sys/vm/drop_caches
EOF

echo "=========================================="
echo "CONFIGURANDO NODO 175"
echo "=========================================="

# Copiar archivo a 175
scp /tmp/clean_ram.sh martin@10.145.0.175:/tmp/

# Conectar a 175 para instalar (Usamos -t para permitir el prompt de password de sudo)
ssh -t martin@10.145.0.175 "sudo mv /tmp/clean_ram.sh /usr/local/bin/clean_ram.sh && sudo chmod +x /usr/local/bin/clean_ram.sh && echo 'martin ALL=(ALL) NOPASSWD: /usr/local/bin/clean_ram.sh' | sudo tee /etc/sudoers.d/clean_ram"

echo ""
echo "=========================================="
echo "CONFIGURANDO NODO 176"
echo "=========================================="

# Copiar archivo a 176
scp /tmp/clean_ram.sh martin@10.145.0.176:/tmp/

# Conectar a 176 para instalar
ssh -t martin@10.145.0.176 "sudo mv /tmp/clean_ram.sh /usr/local/bin/clean_ram.sh && sudo chmod +x /usr/local/bin/clean_ram.sh && echo 'martin ALL=(ALL) NOPASSWD: /usr/local/bin/clean_ram.sh' | sudo tee /etc/sudoers.d/clean_ram"

echo ""
echo "✅ CONFIGURACIÓN FINALIZADA EN AMBOS NODOS"
echo "💡 Ahora puedes probar: python3 quey1.py"
