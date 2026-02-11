<template>
  <div class="selector-ejercicio">
    <h2>Ejercicio</h2>
    <div class="timeline-wrapper">
      <!-- Botón navegar a la izquierda (años más antiguos) -->
      <button
        @click="navegarIzquierda"
        :disabled="!puedeIrIzquierda"
        class="btn-navegacion btn-izquierda"
        title="Ver años anteriores"
      >
        ‹
      </button>

      <div
        class="timeline-container"
        :class="{ 'arrastrando': estoyArrastrando }"
        @mousedown="iniciarArrastre"
        @mousemove="manejarArrastre"
        @mouseup="terminarArrastre"
        @mouseleave="terminarArrastre"
      >
        <!-- Línea gruesa de fondo -->
        <div class="timeline-track"></div>

        <!-- Aros de años -->
        <div class="timeline-years">
          <div
            v-for="(year, index) in ejerciciosVisibles"
            :key="year"
            class="year-circle"
            :class="{ active: modelValue === year }"
            @click="seleccionarEjercicio(year)"
          >
            <div class="circle-ring"></div>
            <span class="year-label">{{ year }}</span>
          </div>
        </div>
      </div>

      <!-- Botón navegar a la derecha (años más recientes) -->
      <button
        @click="navegarDerecha"
        :disabled="!puedeIrDerecha"
        class="btn-navegacion btn-derecha"
        title="Ver años siguientes"
      >
        ›
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  modelValue: {
    type: Number,
    required: true
  },
  ejercicios: {
    type: Array,
    default: () => [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
  },
  añosVisiblesPorPagina: {
    type: Number,
    default: 7
  }
})

const emit = defineEmits(['update:modelValue'])

// Estado para el rango visible
const añoActual = new Date().getFullYear()
const añoInicio = ref(Math.max(Math.min(...props.ejercicios), añoActual - props.añosVisiblesPorPagina + 1))

// Estado para el arrastre
const estoyArrastrando = ref(false)
const posicionInicialX = ref(0)
const añoInicioAlArrastrar = ref(0)

// Computed para los años visibles en el timeline
const ejerciciosVisibles = computed(() => {
  const inicio = añoInicio.value
  const fin = inicio + props.añosVisiblesPorPagina - 1
  return Array.from(
    { length: props.añosVisiblesPorPagina },
    (_, i) => inicio + i
  ).filter(year => year <= añoActual)
})

// Computed para saber si se puede navegar
const puedeIrIzquierda = computed(() => {
  return añoInicio.value > Math.min(...props.ejercicios)
})

const puedeIrDerecha = computed(() => {
  const ultimoVisible = ejerciciosVisibles.value[ejerciciosVisibles.value.length - 1]
  return ultimoVisible < añoActual
})

function seleccionarEjercicio(year) {
  emit('update:modelValue', year)
}

function navegarIzquierda() {
  if (puedeIrIzquierda.value) {
    añoInicio.value -= props.añosVisiblesPorPagina
    if (añoInicio.value < Math.min(...props.ejercicios)) {
      añoInicio.value = Math.min(...props.ejercicios)
    }
  }
}

function navegarDerecha() {
  if (puedeIrDerecha.value) {
    añoInicio.value += props.añosVisiblesPorPagina
    const maxInicio = añoActual - props.añosVisiblesPorPagina + 1
    if (añoInicio.value > maxInicio) {
      añoInicio.value = maxInicio
    }
  }
}

// Funciones para arrastrar
function iniciarArrastre(event) {
  estoyArrastrando.value = true
  posicionInicialX.value = event.clientX
  añoInicioAlArrastrar.value = añoInicio.value
}

function manejarArrastre(event) {
  if (!estoyArrastrando.value) return

  const delta = event.clientX - posicionInicialX.value
  const umbralPixeles = 50 // Píxeles necesarios para cambiar un año
  const cambioAños = Math.round(-delta / umbralPixeles)

  let nuevoInicio = añoInicioAlArrastrar.value + cambioAños

  // Aplicar límites
  const minInicio = Math.min(...props.ejercicios)
  const maxInicio = añoActual - props.añosVisiblesPorPagina + 1
  nuevoInicio = Math.max(minInicio, Math.min(nuevoInicio, maxInicio))

  añoInicio.value = nuevoInicio
}

function terminarArrastre() {
  estoyArrastrando.value = false
}
</script>

<style scoped>
.selector-ejercicio {
  margin-bottom: 32px;
}

.selector-ejercicio h2 {
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary, #ffffff);
  margin: 0 0 20px 0;
}

.timeline-wrapper {
  display: flex;
  align-items: center;
  gap: 16px;
}

.btn-navegacion {
  background: transparent;
  border: 2px solid #4a9eff;
  color: #4a9eff;
  width: 40px;
  height: 40px;
  border-radius: 50%;
  font-size: 24px;
  font-weight: 700;
  cursor: pointer;
  transition: all 0.3s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  flex-shrink: 0;
}

.btn-navegacion:hover:not(:disabled) {
  background: #4a9eff;
  color: #ffffff;
  transform: scale(1.1);
}

.btn-navegacion:disabled {
  border-color: #666666;
  color: #666666;
  cursor: not-allowed;
  opacity: 0.4;
}

.timeline-container {
  position: relative;
  height: 80px;
  padding: 0 40px;
  cursor: grab;
  user-select: none;
  flex: 1;
}

.timeline-container.arrastrando {
  cursor: grabbing;
}

.timeline-track {
  position: absolute;
  top: 30px;
  left: 40px;
  right: 40px;
  height: 3px;
  background: #4a9eff;
  border-radius: 1.5px;
}

.timeline-years {
  position: absolute;
  left: 40px;
  right: 40px;
  top: 0;
  height: 100%;
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
}

.year-circle {
  cursor: pointer;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
}

.circle-ring {
  width: 32px;
  height: 32px;
  border: 3px solid #4a9eff;
  border-radius: 50%;
  background: #f9fafb;
  transition: all 0.3s ease;
  margin-top: 14px;
  position: relative;
  z-index: 1;
}

.year-circle:hover .circle-ring {
  transform: scale(1.15);
}

.year-circle.active .circle-ring {
  background: #4a9eff;
  border-color: #4a9eff;
  box-shadow: 0 0 16px rgba(74, 158, 255, 0.6);
}

.year-label {
  font-size: 14px;
  font-weight: 700;
  color: #999999;
  user-select: none;
  transition: all 0.2s ease;
}

.year-circle:hover .year-label {
  color: #ffffff;
}

.year-circle.active .year-label {
  color: #4a9eff;
  font-size: 15px;
}
</style>
