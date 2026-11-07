import { Component, OnInit, ChangeDetectorRef, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { NotificacionesService } from '../../services/notificaciones';

@Component({
  selector: 'app-notificaciones',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, FormsModule],
  templateUrl: './notificaciones.html',
  styleUrl: './notificaciones.css',
})
export class Notificaciones implements OnInit {
  notificaciones: any[] = [];

  // Estado del modal "Nueva notificación" (productor RabbitMQ)
  modalAbierto = false;
  destinatario = '';
  mensaje = '';
  enviando = false;
  feedback: { tipo: 'ok' | 'error'; texto: string } | null = null;

  private notificacionesService = inject(NotificacionesService);
  private cdr = inject(ChangeDetectorRef);

  ngOnInit(): void {
    this.cargar();
  }

  cargar(): void {
    this.notificacionesService.listar().subscribe({
      next: (data) => {
        this.notificaciones = data;
        this.cdr.detectChanges();
      },
      error: (err) => console.error('Error al obtener las notificaciones:', err)
    });
  }

  abrirModal(): void {
    this.destinatario = '';
    this.mensaje = '';
    this.feedback = null;
    this.modalAbierto = true;
  }

  cerrarModal(): void {
    this.modalAbierto = false;
  }

  enviar(): void {
    const destinatario = this.destinatario.trim();
    const mensaje = this.mensaje.trim();
    if (!destinatario || !mensaje) {
      this.feedback = { tipo: 'error', texto: 'Destinatario y mensaje son obligatorios.' };
      return;
    }

    this.enviando = true;
    this.feedback = null;
    this.notificacionesService.enviar(destinatario, mensaje).subscribe({
      next: () => {
        this.enviando = false;
        this.modalAbierto = false;
        this.feedback = { tipo: 'ok', texto: 'Mensaje enviado a la cola. Se procesará en segundo plano.' };
        this.cdr.detectChanges();
        // El consumidor persiste de forma asíncrona: se recarga la lista tras un instante.
        setTimeout(() => this.cargar(), 1500);
      },
      error: (err) => {
        this.enviando = false;
        this.feedback = {
          tipo: 'error',
          texto: err?.error?.error ?? err?.error?.message ?? 'No se pudo enviar la notificación.'
        };
        this.cdr.detectChanges();
      }
    });
  }
}
