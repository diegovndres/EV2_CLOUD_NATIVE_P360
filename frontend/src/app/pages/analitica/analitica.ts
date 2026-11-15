import { Component, OnDestroy, OnInit, ChangeDetectorRef, inject } from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { AnaliticaService, MetricaProducto } from '../../services/analitica';

@Component({
  selector: 'app-analitica',
  standalone: true,
  imports: [RouterLink, RouterLinkActive, DecimalPipe, DatePipe],
  templateUrl: './analitica.html',
  styleUrl: './analitica.css',
})
export class Analitica implements OnInit, OnDestroy {
  metricas: MetricaProducto[] = [];
  error: string | null = null;
  ultimaActualizacion: Date | null = null;

  private cargando = false;
  private temporizador?: ReturnType<typeof setInterval>;

  private analiticaService = inject(AnaliticaService);
  private cdr = inject(ChangeDetectorRef);

  ngOnInit(): void {
    this.cargar();
    // Los eventos llegan de forma asincrona por Kafka: se refresca cada 10 s.
    this.temporizador = setInterval(() => this.cargar(), 10000);
  }

  ngOnDestroy(): void {
    clearInterval(this.temporizador);
  }

  cargar(): void {
    if (this.cargando) return;
    this.cargando = true;
    this.analiticaService.productos().subscribe({
      next: (data) => {
        this.metricas = data;
        this.error = null;
        this.ultimaActualizacion = new Date();
        this.cargando = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.cargando = false;
        this.error =
          err?.status === 0
            ? 'No se pudo conectar con analitica-service.'
            : `Error al obtener la analítica (HTTP ${err?.status}).`;
        console.error('Error al obtener la analítica:', err);
        this.cdr.detectChanges();
      }
    });
  }

  get totalPedidos(): number {
    return this.metricas.reduce((suma, m) => suma + m.totalPedidos, 0);
  }

  get totalUnidades(): number {
    return this.metricas.reduce((suma, m) => suma + m.unidadesVendidas, 0);
  }

  /** Ancho de la barra: unidades del producto respecto al producto mas vendido. */
  porcentaje(m: MetricaProducto): number {
    const max = Math.max(0, ...this.metricas.map((x) => x.unidadesVendidas));
    return max === 0 ? 0 : Math.round((m.unidadesVendidas / max) * 100);
  }
}