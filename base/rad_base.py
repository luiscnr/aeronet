

class RadiometerBase:

    def add_validation_criteria(self, validation_criteria, wavelength,nwl, criteria):
        if criteria['type'] == 'th':
            if criteria['wlmin'] is None:
                criteria['wlmin'] = wavelength[0]
            if criteria['wlmax'] is None:
                criteria['wlmax'] = wavelength[nwl - 1]
        validation_criteria.append(criteria)
        return validation_criteria

    # Defining thershold to consider a spectra as not valid.
    # thtype: gt (no valid if rrs>thvalue) or lw(no valid if rrs<thvalue)
    def add_th_validation_criteria(self, validation_criteria,wavelength,nwl,wlmin, wlmax, thvalue, thtype):
        if wavelength is not None:
            if wlmin is None:
                wlmin = wavelength[0]
            if wlmax is None:
                wlmax = wavelength[nwl - 1]
        criteria = {
            'type': 'th',
            'wlmin': wlmin,
            'wlmax': wlmax,
            'thvalue': thvalue,
            'thtype': thtype
        }
        validation_criteria.append(criteria)
        return validation_criteria

    def add_time_validation_criteria(self, validation_criteria,hour, minute, diffmax):
        criteria = {
            'type': 'time',
            'hour': hour,
            'minute': minute,
            'diffmax': diffmax
        }
        validation_criteria.append(criteria)
        return validation_criteria