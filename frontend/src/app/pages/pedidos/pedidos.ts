import { Component, OnInit, ChangeDetectorRef, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { NuevoPedido, PedidosService } from '../../services/pedidos';

@Component({
  selector: 'app-pedidos',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, FormsModule],
  templateUrl: './pedidos.html',
  styleUrl: './pedidos.css',
})
export class Pedidos implements OnInit {
  pedidos: any[] = [];

  // Modal "Nuevo pedido"
  mostrarModal = false;
  enviando = false;
  errorFormulario: string | null = null;
  feedback: { tipo: 'exito' | 'error'; mensaje: string } | null = null;

  // Ajusta esta lista a los estados que ya usa tu backend.
  estados = ['CREADO', 'Pendiente', 'En camino', 'Entregado', 'Cancelado'];
  nuevo: NuevoPedido = this.pedidoVacio();

  private pedidosService = inject(PedidosService);
  private cdr = inject(ChangeDetectorRef);

  ngOnInit(): void {
    this.cargar();
  }

  cargar(): void {
    this.pedidosService.listar().subscribe({
      next: (data) => {
        this.pedidos = data;
        this.cdr.detectChanges();
      },
      error: (err) => console.error('Error al obtener los datos:', err)
    });
  }

  abrirModal(): void {
    this.nuevo = this.pedidoVacio();
    this.errorFormulario = null;
    this.mostrarModal = true;
  }

  cerrarModal(): void {
    if (this.enviando) return;
    this.mostrarModal = false;
  }

  guardar(): void {
    const p = this.nuevo;
    if (!p.cliente.trim() || !p.producto.trim() || !p.fecha) {
      this.errorFormulario = 'Cliente, producto y fecha son obligatorios.';
      return;
    }
    if (!Number.isInteger(p.cantidad) || p.cantidad < 1) {
      this.errorFormulario = 'La cantidad debe ser un número entero mayor o igual a 1.';
      return;
    }

    this.errorFormulario = null;
    this.enviando = true;

    this.pedidosService
      .crear({ ...p, cliente: p.cliente.trim(), producto: p.producto.trim() })
      .subscribe({
        next: (creado) => {
          this.enviando = false;
          this.mostrarModal = false;
          this.mostrarFeedback('exito', `Pedido #${creado?.id ?? ''} creado. El evento se publicó en Kafka.`);
          this.cargar();
        },
        error: (err) => {
          this.enviando = false;
          console.error('Error al crear el pedido:', err);
          this.errorFormulario =
            err?.status === 0
              ? 'No se pudo conectar con pedidos-service.'
              : `No se pudo crear el pedido (HTTP ${err?.status}).`;
          this.cdr.detectChanges();
        }
      });
  }

  private mostrarFeedback(tipo: 'exito' | 'error', mensaje: string): void {
    this.feedback = { tipo, mensaje };
    this.cdr.detectChanges();
    setTimeout(() => {
      this.feedback = null;
      this.cdr.detectChanges();
    }, 5000);
  }

  private pedidoVacio(): NuevoPedido {
    return {
      cliente: '',
      producto: '',
      cantidad: 1,
      estado: 'Pendiente',
      fecha: new Date().toISOString().slice(0, 10) // yyyy-MM-dd
    };
  }
}