<script setup>
import Icon from './Icon.vue'

// Standard panel chrome for the dashboard. Wraps the global .card styles so a
// future restyle is a one-file change. Use the `actions` slot for header
// buttons and the default slot for body content.
defineProps({
  title: { type: String, default: '' },
  icon: { type: String, default: '' },
  // Pad the body (default). Set :padded="false" for full-bleed content like
  // tables or charts that manage their own spacing.
  padded: { type: Boolean, default: true },
})
</script>

<template>
  <section class="card panel-card">
    <header v-if="title || $slots.actions || $slots.title" class="card-header">
      <span class="panel-title">
        <Icon v-if="icon" :name="icon" :size="15" />
        <slot name="title">{{ title }}</slot>
      </span>
      <span v-if="$slots.actions" class="panel-actions">
        <slot name="actions" />
      </span>
    </header>
    <div :class="padded ? 'card-body' : ''">
      <slot />
    </div>
  </section>
</template>

<style scoped>
.panel-title {
  display: inline-flex;
  align-items: center;
  gap: var(--space-2);
  color: var(--text-primary);
}
.panel-actions {
  display: inline-flex;
  align-items: center;
  gap: var(--space-2);
}
</style>
