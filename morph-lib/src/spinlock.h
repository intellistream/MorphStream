#ifndef DDCKV_SPINLOCK_H_
#define DDCKV_SPINLOCK_H_

#define barrier() asm volatile("": : :"memory")

#define _cpu_relax() asm volatile("pause\n": : :"memory")

static inline unsigned short xchg_8(void * ptr, unsigned char x) {
    __asm__ __volatile__("xchgb %0,%1"
                         :"=r" (x)
                         :"m" (*(volatile unsigned char *)ptr), "0" (x)
                         :"memory");
    return x;
}

#define BUSY 1
typedef unsigned char spinlock_t;

#define SPINLOCK_INITIALIZER 0

static inline void spin_lock(spinlock_t * lock) {
    while (1) {
        if (!xchg_8(lock, BUSY)) 
            return;

        while (*lock) _cpu_relax();
    }
}

static inline void spin_unlock(spinlock_t * lock) {
    barrier();
    *lock = 0;
}

static inline int spin_trylock(spinlock_t * lock) {
    return xchg_8(lock, BUSY);
}

#endif